# taslite

A sqlite-backed database for tasl instances.

## API

```ts
import * as tasl from "tasl"

export declare class DB {
	// Open an existing database at the given path
	static open(path: string, options?: { readOnly?: boolean }): DB

	// Create a new database at the given path with the given schema.
	// Use path: null for an in-memory sqlite instance
	static create(path: string | null, schema: tasl.Schema): DB

	// Import a tasl instance from an AsyncIterable
	static import(
		path: string | null,
		schema: tasl.Schema,
		stream: AsyncIterable<Buffer>
	): Promise<DB>

	// Close the database
	close(): void

	// Get the database schema
	get schema(): tasl.Schema

	// Export the database to an AsyncIterable
	export(options?: { chunkSize?: number }): AsyncIterable<Buffer>

	// Get an element value
	get(key: string, id: number): tasl.values.Value

	// Iterate over elements ids, values, or [id, value] entries
	keys(key: string): Iterable<number>
	values(key: string): Iterable<tasl.values.Value>
	entries(key: string): Iterable<[number, tasl.values.Value]>

	// Check for the presence of an element with a given id
	has(key: string, id: number): boolean

	// Count the elements of a given class
	count(key: string): number

	// Push a new value of a class using an autoincrementing id, returning the id
	push(key: string, value: tasl.values.Value): number

	set(key: string, id: number, value: tasl.values.Value): void
	merge(elements: Record<string, tasl.values.Element[]>): void
	migrate(mapping: tasl.Mapping, targetPath: string | null): Promise<DB>
}
```

taslite uses [better-sqlite](https://github.com/JoshuaWise/better-sqlite3) for sqlite3 bindings, which has a _synchronous API_ that blocks on the main thread. All the database methods here, except for `DB.import` / `DB.export` and `DB.migrate`, are also synchronous.

## Getting and setting elements

Elements in a tasl instance each have an unsigned integer `id`, unique within each class. The simplest way to use taslite is with the `.get` and `.set` methods.

```ts
import { DB } from "taslite"
import * as tasl from "tasl"

const schema = tasl.parseSchema(`
namespace s http://schema.org/

class s:Person {
	s:name -> string
	s:email -> uri
}
`)

// the path here can also be null to open an in-memory database
const db = DB.create("my-database.sqlite", schema)

// element IDs don't have to be created in order
db.set(
	"http://schema.org/Person",
	19,
	tasl.values.product({
		"http://schema.org/name": tasl.values.string("John Doe"),
		"http://schema.org/email": tasl.values.uri("mailto:johndoe@example.com"),
	})
)

db.set(
	"http://schema.org/Person",
	19103,
	tasl.values.product({
		"http://schema.org/name": tasl.values.string("Jane Doe"),
		"http://schema.org/email": tasl.values.uri("mailto:me@janedoe.com"),
	})
)

db.get("http://schema.org/Person", 19)
// {
//   kind: 'product',
//   components: {
//     'http://schema.org/email': { kind: 'uri', value: 'mailto:johndoe@example.com' },
//     'http://schema.org/name': { kind: 'literal', value: 'John Doe' }
//   }
// }

db.get("http://schema.org/Person", 19103)
// {
//   kind: 'product',
//   components: {
//     'http://schema.org/email': { kind: 'uri', value: 'mailto:me@janedoe.com' },
//     'http://schema.org/name': { kind: 'literal', value: 'Jane Doe' }
//   }
// }

db.get("http://schema.org/Person", 0)
// Uncaught Error: no element in http://schema.org/Person with id 0

db.get("http://schema.org/Book", 0)
// Uncaught Error: key not found
```

## Iterating over elements

Just like `tasl.Instance`, the `DB` class has three methods for iterating (synchronously!) over elements in the database. Iteration always happens in ascending ID order.

```ts
for (const id of db.keys("http://schema.org/Person")) {
	console.log(id)
}
// 19
// 19103

for (const value of db.values("http://schema.org/Person")) {
	console.log(value)
}
// {
//   kind: 'product',
//   components: {
//     'http://schema.org/email': { kind: 'uri', value: 'mailto:johndoe@example.com' },
//     'http://schema.org/name': { kind: 'literal', value: 'John Doe' }
//   }
// }
// {
//   kind: 'product',
//   components: {
//     'http://schema.org/email': { kind: 'uri', value: 'mailto:me@janedoe.com' },
//     'http://schema.org/name': { kind: 'literal', value: 'Jane Doe' }
//   }
// }

for (const [id, value] of db.entries("http://schema.org/Person")) {
	console.log(id, value)
}
// 19 {
//   kind: 'product',
//   components: {
//     'http://schema.org/email': { kind: 'uri', value: 'mailto:johndoe@example.com' },
//     'http://schema.org/name': { kind: 'literal', value: 'John Doe' }
//   }
// }
// 19103 {
//   kind: 'product',
//   components: {
//     'http://schema.org/email': { kind: 'uri', value: 'mailto:me@janedoe.com' },
//     'http://schema.org/name': { kind: 'literal', value: 'Jane Doe' }
//   }
// }
```

## Merging many elements at once

Classes in a tasl schema can reference each other, which makes for awkward DX when trying to update them. For example, here's a valid tasl schema:

```ts
import * as tasl from "tasl"

const schema = tasl.parseSchema(`
namespace ex http://example.com/

class ex:Person {
	ex:name -> string
	ex:favoriteBook -> * ex:Book
}

class ex:Book {
	ex:name -> string
	ex:author -> * ex:Person
}
`)
```

Here we have two classes, each of whose types reference the other. Let's try creating a database with this schema inserting some elements:

```ts
const db = DB.create("my-database.sqlite", schema)

db.set(
	"http://example.com/Person",
	100,
	tasl.values.product({
		"http://example.com/name": tasl.values.string("John Doe"),
		"http://example.com/favoriteBook": tasl.values.reference(0),
	})
)
// SqliteError: FOREIGN KEY constraint failed
```

That's because every element value of `ex:Person` needs to reference an element value of `ex:Book` - but there are no `ex:Book` elements yet! The only way around this is to add elements to both classes at the same time, which we can do using the `DB.merge` method.

```ts
db.merge({
	"http://example.com/Person": [
		{
			id: 100,
			value: tasl.values.product({
				"http://example.com/name": tasl.values.string("John Doe"),
				"http://example.com/favoriteBook": tasl.values.reference(0),
			}),
		},
	],
	"http://example.com/Book": [
		{
			id: 0,
			value: tasl.values.product({
				"http://example.com/name": tasl.values.string(
					"John Doe: My Autobiography"
				),
				"http://example.com/author": tasl.values.reference(100),
			}),
		},
	],
})
```

## More examples

Create a new database with a given schema

```ts
import fs from "node:fs"

import { DB } from "taslite"
import * as tasl from "tasl"

const schemaPath = "./test/instances/micro.schema"

const schema = tasl.parseSchema(fs.readFileSync(schemaPath, "utf-8"))

const db = DB.create("micro.sqlite", schema)
```

Open an existing `.sqlite` database

```ts
import fs from "node:fs"

import { DB } from "taslite"
import * as tasl from "tasl"

const databasePath = "example.sqlite"

const db = DB.open("example.sqlite")
```

Import an existing instance from a `.instance` file

```ts
import fs from "node:fs"

import { DB } from "taslite"
import * as tasl from "tasl"

const schemaPath = "./example.schema"
const instancePath = "./example.instance"

const schema = tasl.parseSchema(fs.readFileSync(schemaPath, "utf-8"))
const db = await DB.import(
	"example.sqlite",
	schema,
	fs.createReadStream(instancePath)
)
```

Apply a migration to an existing database

```ts
import * as tasl from "tas"
import assert from "node:assert"

const sourceSchema = tasl.parseSchema(`
namespace s http://schema.org/
class s:Person {
	s:name   -> string
	s:gender -> [
		s:Male
		s:Female
		s:value <- string
	]
}`)

const targetSchema = new tasl.Schema({
	"http://example.com/person": tasl.types.product({
		"http://example.com/name": tasl.types.string,
		"http://example.com/gender": tasl.types.string,
	}),
})

const targetSchema = tasl.parseSchema(`
namespace ex http://example.com/

class ex:person {
	ex:name   -> string
	ex:gender -> string
}`)

const mapping = tasl.parseMapping(
	sourceSchema,
	targetSchema,
	`
namespace s http://schema.org/
namepsace ex http://example.com/

map ex:person <= s:Person (p) => {
	ex:name   <= p / s:name
	ex:gender <= p / s:gender [
		s:Male   (u) => "Male"
		s:Female (u) => "Female"
		s:value  (v) => v
	]
}
`
)

const sourceDB = DB.open("./path/to/source.sqlite")
assert(sourceDB.schema.isEqualTo(sourceSchema))

const targetDB = await sourceDB.migrate(mapping, "./path/to/target.sqlite")
```
