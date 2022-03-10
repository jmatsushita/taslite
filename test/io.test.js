import test from "ava"
import * as tasl from "tasl"

import { DB } from "../lib/db.js"
import { microInstance, microSchema } from "./instances/micro.js"

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

test("Round-trip micro instance byte-by-byte", async (t) => {
	const data = Buffer.from(tasl.encodeInstance(microInstance))

	const db = await DB.import(null, microSchema, forBytes(data))

	const chunks = []
	for await (const chunk of db.export()) {
		chunks.push(chunk)
	}

	t.deepEqual(Buffer.concat(chunks), data)
})

test("Round-trip micro instance in randomly-sized chunks", async (t) => {
	const data = Buffer.from(tasl.encodeInstance(microInstance))

	const db = await DB.import(null, microSchema, forByteInChunks(data))

	const chunks = []
	for await (const chunk of db.export()) {
		chunks.push(chunk)
	}

	t.deepEqual(Buffer.concat(chunks), data)
})
