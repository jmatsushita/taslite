import varint from "varint"
import * as microcbor from "microcbor"
import { types, forComponents, optionAtIndex, forOptions } from "tasl"
import {
	fixedSizeLiterals,
	getProperties,
	getPropertyName,
	Values,
} from "./utils.js"
import { rdf, xsd } from "@underlay/namespaces"

/**
 * We're implementing a static DB.import(): DB method for creating *and populating*
 * a database from an async iterator. The async iterator gives us Buffers in chunks,
 * and our goal is to stream them directly into the database.
 *
 * We do this in two stages: first, we "re-chunk" the stream around element value
 * boundaries, which involves traversing the expected type and feeding chunks
 * into an interal buffer. Then we allocate a new ArrayBuffer for the element value,
 * and walk the type again, this time parsing the individual leafs into a row object.
 */

type ParseState = { buffer: Buffer; offset: number }

export class Decoder {
	private chunks: Buffer[]
	private lastChunk: Buffer | null
	private byteLength: number
	private endOffset: number
	private startOffset: number
	public readonly iterator: AsyncIterator<Buffer>

	private static MSB = 0x80
	private static REST = 0x7f

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
				type.datatype in fixedSizeLiterals
					? fixedSizeLiterals[type.datatype]
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

	public async decodeElement(type: types.Type): Promise<Buffer> {
		await this.readValue(type)
		const buffer = this.collect()
		this.flush()

		return buffer
	}

	public async decodeRow(type: types.Type): Promise<Values> {
		const buffer = await this.decodeElement(type)
		const row: Values = {}
		Decoder.parseRow({ buffer, offset: 0 }, type, [], row)
		return row
	}

	private static parseString(state: ParseState): string {
		const n = varint.decode(state.buffer, state.offset)
		state.offset += varint.encodingLength(n)
		const value = state.buffer
			.subarray(state.offset, state.offset + n)
			.toString("utf-8")
		state.offset += n
		return value
	}

	private static parseLiteral(
		state: ParseState,
		datatype: string
	): string | number | Buffer {
		if (datatype === xsd.boolean) {
			const value = state.buffer.readUint8(state.offset)
			state.offset += 1
			return value
		} else if (datatype === xsd.double) {
			const value = state.buffer.readDoubleBE(state.offset)
			state.offset += 8
			return value
		} else if (datatype === xsd.float) {
			const value = state.buffer.readFloatBE(state.offset)
			state.offset += 4
			return value
		} else if (datatype === xsd.long) {
			const value = state.buffer.readBigInt64BE(state.offset)
			if (value > Number.MAX_SAFE_INTEGER) {
				throw new Error(
					"taslite does not support i64 values greater than Number.MAX_SAFE_INTEGER"
				)
			}

			state.offset += 8
			return Number(value)
		} else if (datatype === xsd.int) {
			const value = state.buffer.readInt32BE(state.offset)
			state.offset += 4
			return value
		} else if (datatype === xsd.short) {
			const value = state.buffer.readInt16BE(state.offset)
			state.offset += 2
			return value
		} else if (datatype === xsd.byte) {
			const value = state.buffer.readInt8(state.offset)
			state.offset += 1
			return value
		} else if (datatype === xsd.unsignedLong) {
			const value = state.buffer.readBigUInt64BE(state.offset)
			if (value > Number.MAX_SAFE_INTEGER) {
				throw new Error(
					"taslite does not support u64 values greater than Number.MAX_SAFE_INTEGER"
				)
			} else if (value < Number.MIN_SAFE_INTEGER) {
				throw new Error(
					"taslite does not support u64 values less than Number.MIN_SAFE_INTEGER"
				)
			}

			state.offset += 8
			return Number(value)
		} else if (datatype === xsd.unsignedInt) {
			const value = state.buffer.readUint32BE(state.offset)
			state.offset += 4
			return value
		} else if (datatype === xsd.unsignedShort) {
			const value = state.buffer.readUint16BE(state.offset)
			state.offset += 2
			return value
		} else if (datatype === xsd.unsignedByte) {
			const value = state.buffer.readUint8(state.offset)
			state.offset += 1
			return value
		} else if (datatype === xsd.hexBinary) {
			const n = varint.decode(state.buffer, state.offset)
			state.offset += varint.encodingLength(n)
			const value = state.buffer.subarray(state.offset, state.offset + n)
			state.offset += n
			return value
		} else if (datatype === rdf.JSON) {
			const n = varint.decode(state.buffer, state.offset)
			state.offset += varint.encodingLength(n)
			const value = microcbor.decode(
				state.buffer.subarray(state.offset, state.offset + n)
			)
			state.offset += n
			return JSON.stringify(value)
		} else {
			return Decoder.parseString(state)
		}
	}

	private static parseRow(
		state: ParseState,
		type: types.Type,
		path: number[],
		row: Values
	) {
		const name = getPropertyName(path)
		if (type.kind === "uri") {
			row[name] = Decoder.parseString(state)
		} else if (type.kind === "literal") {
			row[name] = Decoder.parseLiteral(state, type.datatype)
		} else if (type.kind === "product") {
			for (const [_, component, index] of forComponents(type)) {
				Decoder.parseRow(state, component, [...path, index], row)
			}
		} else if (type.kind === "coproduct") {
			const index = varint.decode(state.buffer, state.offset)
			state.offset += varint.encodingLength(index)
			row[name] = index
			for (const [key, option, i] of forOptions(type)) {
				const optionPath = [...path, i]
				if (index === i) {
					Decoder.parseRow(state, option, optionPath, row)
				} else {
					for (const [path] of getProperties(option, optionPath)) {
						const name = getPropertyName(path)
						row[name] = null
					}
				}
			}
		} else if (type.kind === "reference") {
			const name = getPropertyName(path)
			const index = varint.decode(state.buffer, state.offset)
			state.offset += varint.encodingLength(index)
			row[name] = index
		} else {
			throw new Error("internal error: invalid type")
		}
	}

	public async *forElements(type: types.Type): AsyncIterable<[number, Buffer]> {
		let id = 0
		const count = await this.decodeUnsignedVarint()
		for (let n = 0; n < count; n++) {
			id += await this.decodeUnsignedVarint()
			const element = await this.decodeElement(type)
			yield [id, element]
			id++
		}
	}

	public async *forRows(type: types.Type): AsyncIterable<Values> {
		for await (const [id, buffer] of this.forElements(type)) {
			const row: Values = { id }
			Decoder.parseRow({ buffer, offset: 0 }, type, [], row)
			yield row
		}
	}
}
