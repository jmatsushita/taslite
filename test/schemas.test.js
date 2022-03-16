import test from "ava"

import Database from "better-sqlite3"

import { DB } from "../lib/db.js"

import { microSchema } from "./instances/micro.js"
import { nanoSchema } from "./instances/nano.js"

test("Nano", (t) => {
	const db = new Database(":memory:")

	DB.setSchema(db, nanoSchema)

	const s = DB.getSchema(db)
	t.true(s.isEqualTo(nanoSchema))
})

test("Micro", (t) => {
	const db = new Database(":memory:")

	DB.setSchema(db, microSchema)

	const s = DB.getSchema(db)
	t.true(s.isEqualTo(microSchema))
})
