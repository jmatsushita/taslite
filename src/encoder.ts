import varint from "varint"
import * as microcbor from "microcbor"
import { rdf, xsd } from "@underlay/namespaces"
import { forComponents, optionAtIndex, types } from "tasl"

import { fixedSizeLiterals, getPropertyName, Values } from "./utils.js"

export class Encoder {
	public static minChunkSize = 8
	public static defaultChunkSize = 1024

	public readonly chunkSize: number
	private readonly buffer: Buffer
	private offset: number = 0
	#closed = false

	constructor(options: { chunkSize?: number } = {}) {
		this.chunkSize = options.chunkSize || Encoder.defaultChunkSize
		if (!Number.isSafeInteger(this.chunkSize)) {
			throw new Error("encoder chunk size must be an integer")
		} else if (this.chunkSize < Encoder.minChunkSize) {
			throw new Error(
				`encoder chunk size must be at least ${Encoder.minChunkSize} bytes`
			)
		}

		this.buffer = Buffer.alloc(this.chunkSize)
	}

	private flush(): Buffer {
		const chunk = Buffer.alloc(this.offset)
		this.buffer.copy(chunk, 0, 0, this.offset)
		this.offset = 0
		return chunk
	}

	private async *allocate(size: number): AsyncIterable<Buffer> {
		if (size > this.chunkSize - this.offset) {
			yield this.flush()
		}
	}

	public async *encodeUnsignedVarint(value: number): AsyncIterable<Buffer> {
		if (this.#closed) {
			throw new Error("stream closed")
		}

		const length = varint.encodingLength(value)
		yield* this.allocate(length)

		varint.encode(value, this.buffer, this.offset)
		this.offset += length
	}

	private async *encodeBuffer(value: Buffer): AsyncIterable<Buffer> {
		let sourceOffset = 0

		let remainingTargetCapacity = this.chunkSize - this.offset

		while (value.byteLength - sourceOffset > remainingTargetCapacity) {
			if (remainingTargetCapacity > 0) {
				value.copy(
					this.buffer,
					this.offset,
					sourceOffset,
					sourceOffset + remainingTargetCapacity
				)

				sourceOffset += this.chunkSize - this.offset
				this.offset = this.chunkSize
			}

			yield this.flush()
			remainingTargetCapacity = this.chunkSize
		}

		if (sourceOffset < value.byteLength) {
			value.copy(this.buffer, this.offset, sourceOffset, value.byteLength)
			this.offset += value.byteLength - sourceOffset
		}
	}

	private async *encodeString(value: string) {
		if (this.#closed) {
			throw new Error("stream closed")
		}

		const data = Buffer.from(value, "utf-8")
		yield* this.encodeUnsignedVarint(data.length)
		yield* this.encodeBuffer(data)
	}

	private async *encodeFizedSizeLiteral(datatype: string, value: number) {
		if (this.#closed) {
			throw new Error("stream closed")
		}

		const size = fixedSizeLiterals[datatype]
		if (size === undefined) {
			throw new Error("invalid integer datatype")
		}

		yield* this.allocate(size)

		if (datatype === xsd.boolean) {
			this.buffer.writeUInt8(value, this.offset)
		} else if (datatype === xsd.double) {
			this.buffer.writeDoubleBE(value, this.offset)
		} else if (datatype === xsd.float) {
			this.buffer.writeFloatBE(value, this.offset)
		} else if (datatype === xsd.long) {
			this.buffer.writeBigInt64BE(BigInt(value), this.offset)
		} else if (datatype === xsd.int) {
			this.buffer.writeInt32BE(value, this.offset)
		} else if (datatype === xsd.short) {
			this.buffer.writeInt16BE(value, this.offset)
		} else if (datatype === xsd.byte) {
			this.buffer.writeInt8(value, this.offset++)
		} else if (datatype === xsd.unsignedLong) {
			this.buffer.writeBigUint64BE(BigInt(value), this.offset)
		} else if (datatype === xsd.unsignedInt) {
			this.buffer.writeUint32BE(value, this.offset)
		} else if (datatype === xsd.unsignedShort) {
			this.buffer.writeUint16BE(value, this.offset)
		} else if (datatype === xsd.unsignedByte) {
			this.buffer.writeUInt8(value, this.offset)
		} else {
			throw new Error("invalid fixed-size literal datatype")
		}

		this.offset += size
	}

	public async *encodeValue(
		type: types.Type,
		path: number[],
		row: Values
	): AsyncIterable<Buffer> {
		if (type.kind === "uri") {
			const propertyName = getPropertyName(path)
			const value = row[propertyName]
			if (typeof value !== "string") {
				throw new Error("internal error: unexpected type")
			}

			yield* this.encodeString(value)
		} else if (type.kind === "literal") {
			const propertyName = getPropertyName(path)
			const value = row[propertyName]

			if (type.datatype in fixedSizeLiterals) {
				if (typeof value !== "number") {
					throw new Error("internal error: unexpected type")
				}

				yield* this.encodeFizedSizeLiteral(type.datatype, value)
			} else if (type.datatype === xsd.hexBinary) {
				if (!Buffer.isBuffer(value)) {
					throw new Error("internal error: unexpected type")
				}

				yield* this.encodeUnsignedVarint(value.byteLength)
				yield* this.encodeBuffer(value)
			} else if (type.datatype === rdf.JSON) {
				if (typeof value !== "string") {
					throw new Error("internal error: unexpected type")
				}

				const array = microcbor.encode(JSON.parse(value), { strictJSON: true })
				const data = Buffer.from(
					array.buffer,
					array.byteOffset,
					array.byteLength
				)

				yield* this.encodeUnsignedVarint(data.byteLength)
				yield* this.encodeBuffer(data)
			} else {
				if (typeof value !== "string") {
					throw new Error("internal error: unexpected type")
				}

				yield* this.encodeString(value)
			}
		} else if (type.kind === "product") {
			for (const [_, component, index] of forComponents(type)) {
				yield* this.encodeValue(component, [...path, index], row)
			}
		} else if (type.kind === "coproduct") {
			const propertyName = getPropertyName(path)
			const index = row[propertyName]
			if (typeof index !== "number") {
				throw new Error("internal error: unexpected type")
			}

			const [_, option] = optionAtIndex(type, index)

			yield* this.encodeUnsignedVarint(index)
			yield* this.encodeValue(option, [...path, index], row)
		} else if (type.kind === "reference") {
			const propertyName = getPropertyName(path)
			const index = row[propertyName]
			if (typeof index !== "number") {
				throw new Error("internal error: unexpected type")
			}

			yield* this.encodeUnsignedVarint(index)
		} else {
			throw new Error("invalid type")
		}
	}

	public async *close(): AsyncIterable<Buffer> {
		if (this.#closed) {
			throw new Error("stream closed")
		}

		if (this.offset > 0) {
			yield this.flush()
		}

		this.#closed = true
	}
}
