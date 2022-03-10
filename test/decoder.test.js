import test from "ava"
import varint from "varint"
import * as tasl from "tasl"

import { Decoder } from "../lib/decoder.js"

import { microSchema, microInstance } from "./instances/micro.js"

async function* forBytes(buffer) {
	for (let i = 0; i < buffer.byteLength; i++) {
		yield Buffer.from([buffer[i]])
	}
}

const maxChunk = 6

async function* forByteInChunks(buffer) {
	let offset = 0
	do {
		const chunkSize = Math.min(
			Math.ceil(Math.random() * maxChunk),
			buffer.byteLength - offset
		)

		const chunk = Buffer.alloc(chunkSize)
		buffer.copy(chunk, 0, offset, offset + chunkSize)
		yield chunk
		offset += chunkSize
	} while (offset < buffer.byteLength)
}

const numbers = [
	0,
	5,
	127,
	128,
	255,
	9,
	123,
	256,
	102899,
	4821908412,
	38921839100382,
	Number.MAX_SAFE_INTEGER,
]

test("Decode unsigned varints byte-by-byte", async (t) => {
	const byteLength = numbers.reduce(
		(byteLength, n) => byteLength + varint.encodingLength(n),
		0
	)

	const buffer = Buffer.alloc(byteLength)
	let byteOffset = 0
	for (const n of numbers) {
		varint.encode(n, buffer, byteOffset)
		byteOffset += varint.encodingLength(n)
	}

	const decoder = new Decoder(forBytes(buffer))

	for (const number of numbers) {
		const n = await decoder.decodeUnsignedVarint()
		t.is(n, number)
	}
})

test("Decoding unsigned varints in randomly-sized chunks", async (t) => {
	const byteLength = numbers.reduce(
		(byteLength, n) => byteLength + varint.encodingLength(n),
		0
	)

	const buffer = Buffer.alloc(byteLength)
	let byteOffset = 0
	for (const n of numbers) {
		varint.encode(n, buffer, byteOffset)
		byteOffset += varint.encodingLength(n)
	}

	const decoder = new Decoder(forByteInChunks(buffer))

	for (const number of numbers) {
		const n = await decoder.decodeUnsignedVarint()
		t.is(n, number)
	}
})

const blocks = [
	Buffer.from([0, 1, 4]),
	Buffer.from([1]),
	Buffer.from([0, 0, 0, 0]),
	Buffer.from([8, 8]),
	Buffer.from([8, 8, 1, 1, 7, 5, 3, 1]),
	Buffer.from("hello world", "utf-8"),
	Buffer.from(
		"hello world this is a longer piece of text that i have written",
		"utf-8"
	),
	Buffer.from(
		"hello world this is a much longer string to test what it's like when there is a really long string that has to be handled by somebody and i hope they did a good job doing it because this is how we find out",
		"utf-8"
	),
]

test("Decoding a series of fixed-sized blocks byte-by-byte", async (t) => {
	const byteLength = blocks.reduce(
		(byteLength, block) => byteLength + block.length,
		0
	)

	const buffer = Buffer.alloc(byteLength)
	let byteOffset = 0
	for (const block of blocks) {
		block.copy(buffer, byteOffset)
		byteOffset += block.byteLength
	}

	const decoder = new Decoder(forBytes(buffer))
	for (const block of blocks) {
		await decoder.skip(block.byteLength)
		const result = decoder.collect()
		t.deepEqual(result, block)
		decoder.flush()
	}
})

test("Decoding a series of fixed-sized blocks in randomly-sized chunks", async (t) => {
	const byteLength = blocks.reduce(
		(byteLength, block) => byteLength + block.length,
		0
	)

	const buffer = Buffer.alloc(byteLength)
	let byteOffset = 0
	for (const block of blocks) {
		buffer.set(block, byteOffset)
		byteOffset += block.length
	}

	const decoder = new Decoder(forByteInChunks(buffer))
	for (const block of blocks) {
		await decoder.skip(block.byteLength)
		const result = decoder.collect()
		t.deepEqual(result, block)
		decoder.flush()
	}
})

// test("Decoding an instance byte-by-byte", async (t) => {
// 	const data = Buffer.from(tasl.encodeInstance(microInstance))

// 	const decoder = new Decoder(forBytes(data))

// 	const version = await decoder.decodeUnsignedVarint()
// 	if (version !== tasl.version) {
// 		throw new Error("unsupported encoding verison")
// 	}

// 	const elements = {}
// 	for (const [key, type] of microSchema.entries()) {
// 		elements[key] = new Map()
// 		let id = 0
// 		const count = await decoder.decodeUnsignedVarint()
// 		for (let n = 0; n < count; n++) {
// 			id += await decoder.decodeUnsignedVarint()
// 			const value = await decoder.decodeValue(type)
// 			elements[key].set(id, value)
// 			id++
// 		}
// 	}

// 	const uri = Buffer.from(
// 		"dweb:/ipfs/bafybeiczsscdsbs7ffqz55asqdf3smv6klcw3gofszvwlyarci47bgf354"
// 	)

// 	t.deepEqual(elements, {
// 		"http://example.com/a": new Map([[0, Buffer.from([0xff, 0x00])]]),
// 		"http://example.com/b": new Map([
// 			[0, Buffer.from([0x00, 0x04, ...[0x0f, 0xee, 0x12, 0x00]])],
// 			[1, Buffer.from([0x01])],
// 			[2, Buffer.from([0x01])],
// 			[3, Buffer.from([0x02, uri.byteLength, ...uri])],
// 		]),
// 	})
// })

test("Decoding an instance in randomly-sized chunks", async (t) => {
	const data = Buffer.from(tasl.encodeInstance(microInstance))

	const decoder = new Decoder(forByteInChunks(data))

	const version = await decoder.decodeUnsignedVarint()
	if (version !== tasl.version) {
		throw new Error("unsupported encoding verison")
	}

	const elements = {}
	for (const [key, type] of microSchema.entries()) {
		elements[key] = new Map()
		for await (const [id, value] of decoder.forElements(type)) {
			elements[key].set(id, value)
		}
	}

	const uri = Buffer.from(
		"dweb:/ipfs/bafybeiczsscdsbs7ffqz55asqdf3smv6klcw3gofszvwlyarci47bgf354"
	)

	t.deepEqual(elements, {
		"http://example.com/a": new Map([[0, Buffer.from([0xff, 0x00])]]),
		"http://example.com/b": new Map([
			[0, Buffer.from([0x00, 0x04, ...[0x0f, 0xee, 0x12, 0x00]])],
			[1, Buffer.from([0x01])],
			[2, Buffer.from([0x01])],
			[3, Buffer.from([0x02, uri.byteLength, ...uri])],
		]),
	})
})
