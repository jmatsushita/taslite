import { xsd } from "@underlay/namespaces"
import { types, forComponents, optionAtIndex } from "tasl"

/**
 * OK here's what's going on.
 *
 * We're implementing a static DB.import(): DB method for creating *and populating*
 * a database from an async iterator. The async iterator gives us Buffers in chunks,
 * and our goal is to stream them directly into the database.
 *
 * We're storing values as byte arrays anyway, so we don't actually need to "parse"
 * values. We really only need to be scanning the stream as it comes in, parsing the
 * element counts and element IDs, and tracking the start and end of element values.
 *
 * This means the object consuming the async iterable needs to be able to do two things:
 * read an unsigned varint from the stream, and read a value from the stream. We don't
 * want to make any assumptions about the buffer chunks we get from the async iterable,
 * so either other these (values and uvarints) might be split across several chunks.
 *
 * Our state, then, is an array of buffered chunks, along with two pointers tracking the
 * beginning and current location of the current scan. The beginning is represented by
 * Decoder.startOffset, which is an index into the first chunk of the array (Decoder.chunks[0]),
 * and end is represented by Decoder.endOffset, which an index into the last chunk in the array.
 * The first an last chunks are the same if Decoder.chunks.length === 1. If there are no
 * chunks in the array, then startOffset and endOffset are both NaN. For convenience, we also
 * maintain Decoder.byteLength: number, which is the total byte length of the current range,
 * as well as Decoder.lastChunk: null | Buffer, which is a pointer to the last chunk in the array,
 * if it exists.
 *
 * When scanning, we need to a) parse uvarints for element counts and IDs, after which the
 * scanned range can be discarded, and b) scan values, whose scanned range needs to be copied
 * into a newly allocated Buffer for insertion into SQLite. Fortunely for us, scanning values
 * only requires two kinds of operations itself: parsing uvarints (retuning the parsed number
 * but NOT discarded the range) and scanning a fixed number of bytes. In other words, all
 * parts of a value are either fixed size, or are prefixed with a uvarint length.
 */

export class Decoder {
	private chunks: Buffer[]
	private lastChunk: Buffer | null
	private byteLength: number
	private endOffset: number
	private startOffset: number
	public readonly iterator: AsyncIterator<Buffer>

	private static MSB = 0x80
	private static REST = 0x7f

	private static fixedSizeLiterals: Record<string, number> = {
		[xsd.boolean]: 1,
		[xsd.double]: 8,
		[xsd.float]: 4,
		[xsd.long]: 8,
		[xsd.int]: 4,
		[xsd.short]: 2,
		[xsd.byte]: 1,
		[xsd.unsignedLong]: 8,
		[xsd.unsignedInt]: 4,
		[xsd.unsignedShort]: 2,
		[xsd.unsignedByte]: 1,
	}

	constructor(stream: AsyncIterable<Buffer>) {
		this.chunks = []
		this.lastChunk = null
		this.startOffset = NaN
		this.endOffset = NaN
		this.byteLength = 0
		this.iterator = stream[Symbol.asyncIterator]()
	}

	private flush() {
		if (this.lastChunk !== null) {
			if (this.endOffset < this.lastChunk.byteLength) {
				this.chunks = [this.lastChunk]
				this.startOffset = this.endOffset
				this.byteLength = 0
			} else {
				this.chunks = []
				this.lastChunk = null
				this.startOffset = 0
				this.endOffset = 0
				this.byteLength = 0
			}
		}
	}

	private collect(): Buffer {
		if (this.lastChunk === null) {
			return Buffer.alloc(0)
		}

		const buffer = Buffer.alloc(this.byteLength)
		let sourceOffset = this.startOffset
		let targetOffset = 0
		for (const chunk of this.chunks) {
			chunk.copy(buffer, targetOffset, sourceOffset)
			targetOffset += chunk.byteLength - sourceOffset
			sourceOffset = 0
		}

		return buffer
	}

	private async getLastChunk(): Promise<Buffer> {
		if (this.lastChunk !== null) {
			return this.lastChunk
		}

		const chunk = await this.next()
		this.chunks.push(chunk)
		this.startOffset = 0
		this.endOffset = 0
		this.lastChunk = chunk
		return chunk
	}

	private async next(): Promise<Buffer> {
		const { value, done } = await this.iterator.next()
		if (done) {
			throw new Error("could not decode stream: iterable ended early")
		} else if (!Buffer.isBuffer(value)) {
			throw new Error("could not decode stream: value is not a Buffer")
		} else if (value.byteLength === 0) {
			throw new Error(
				"could not decode stream: buffer chunks cannot be length zero"
			)
		}

		return value
	}

	private async skip(n: number) {
		const initialByteLength = this.byteLength
		const targetByteLength = initialByteLength + n

		let lastChunk = await this.getLastChunk()

		// TODO: think about better ways to write this loop
		while (this.byteLength < targetByteLength) {
			const remainingCapacity = lastChunk.byteLength - this.endOffset
			if (this.byteLength + remainingCapacity < targetByteLength) {
				lastChunk = await this.next()
				this.chunks.push(lastChunk)
				this.lastChunk = lastChunk
				this.endOffset = 0
				this.byteLength += remainingCapacity
			} else {
				this.endOffset += targetByteLength - this.byteLength
				this.byteLength = targetByteLength
			}
		}
	}

	private async readUnsignedVarint(): Promise<number> {
		let [result, shift, byte] = [0, 0, 0]

		let lastChunk = await this.getLastChunk()

		do {
			if (shift > 49) {
				throw new RangeError("could not decode unsigned varint")
			}

			if (this.endOffset === lastChunk.byteLength) {
				lastChunk = await this.next()
				this.chunks.push(lastChunk)
				this.lastChunk = lastChunk
				this.endOffset = 0
			}

			this.byteLength++
			byte = lastChunk[this.endOffset++]

			result +=
				shift < 28
					? (byte & Decoder.REST) << shift
					: (byte & Decoder.REST) * Math.pow(2, shift)
			shift += 7
		} while (byte >= Decoder.MSB)

		return result
	}

	private async readValue(type: types.Type): Promise<void> {
		if (type.kind === "uri") {
			const length = await this.readUnsignedVarint()
			await this.skip(length)
		} else if (type.kind === "literal") {
			// Every literal is either a fixed size literal (boolean, numbers),
			// or it is prefixed with a uvarint length (string, JSON, binary, and all others)
			const length =
				type.datatype in Decoder.fixedSizeLiterals
					? Decoder.fixedSizeLiterals[type.datatype]
					: await this.readUnsignedVarint()
			await this.skip(length)
		} else if (type.kind === "product") {
			for (const [_, component] of forComponents(type)) {
				await this.readValue(component)
			}
		} else if (type.kind === "coproduct") {
			const index = await this.readUnsignedVarint()
			const [_, option] = optionAtIndex(type, index)
			await this.readValue(option)
		} else if (type.kind === "reference") {
			await this.readUnsignedVarint()
		} else {
			throw new Error("type error")
		}
	}

	public async decodeUnsignedVarint(): Promise<number> {
		const n = await this.readUnsignedVarint()
		this.flush()
		return n
	}

	public async decodeValue(type: types.Type): Promise<Buffer> {
		await this.readValue(type)
		const value = this.collect()
		this.flush()
		return value
	}

	public async *forElements(type: types.Type): AsyncIterable<[number, Buffer]> {
		let id = 0
		const count = await this.decodeUnsignedVarint()
		for (let n = 0; n < count; n++) {
			id += await this.decodeUnsignedVarint()
			const value = await this.decodeValue(type)
			yield [id, value]
			id++
		}
	}
}
