import varint from "varint"

const minChunkSize = varint.encodingLength(Number.MAX_SAFE_INTEGER)

export class Encoder {
	readonly #buffer: Buffer
	#offset: number = 0
	#closed = false

	constructor(private readonly chunkSize: number) {
		if (!Number.isSafeInteger(this.chunkSize)) {
			throw new Error("encoder chunk size must be an integer")
		} else if (this.chunkSize < minChunkSize) {
			throw new Error(
				`encoder chunk size must be at least ${minChunkSize} bytes`
			)
		}

		this.#buffer = Buffer.alloc(this.chunkSize)
	}

	private flush(): Buffer {
		const chunk = Buffer.alloc(this.#offset)
		this.#buffer.copy(chunk, 0, 0, this.#offset)
		this.#offset = 0
		return chunk
	}

	public async *encodeUnsignedVarint(value: number): AsyncIterable<Buffer> {
		if (this.#closed) {
			throw new Error("stream closed")
		}

		const length = varint.encodingLength(value)
		if (length > this.chunkSize - this.#offset) {
			yield this.flush()
		}

		varint.encode(value, this.#buffer, this.#offset)
		this.#offset += length
	}

	public async *encodeValue(value: Buffer): AsyncIterable<Buffer> {
		if (this.#closed) {
			throw new Error("stream closed")
		}

		let sourceOffset = 0

		let remainingTargetCapacity = this.chunkSize - this.#offset

		while (value.byteLength - sourceOffset > remainingTargetCapacity) {
			if (remainingTargetCapacity > 0) {
				value.copy(
					this.#buffer,
					this.#offset,
					sourceOffset,
					sourceOffset + remainingTargetCapacity
				)

				sourceOffset += this.chunkSize - this.#offset
				this.#offset = this.chunkSize
			}

			yield this.flush()
			remainingTargetCapacity = this.chunkSize
		}

		if (sourceOffset < value.byteLength) {
			value.copy(this.#buffer, this.#offset, sourceOffset, value.byteLength)
			this.#offset += value.byteLength - sourceOffset
		}
	}

	public async *close(): AsyncIterable<Buffer> {
		if (this.#closed) {
			throw new Error("stream closed")
		}

		if (this.#offset > 0) {
			yield this.flush()
		}

		this.#closed = true
	}
}
