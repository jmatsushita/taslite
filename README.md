# node-sqlite-tasl

A sqlite-backed database for tasl instances

## API

```ts
import * as tasl from "tasl"

declare class DB {
	// Open an existing database at the given path
	public static openDB(
		path: string | null,
		options?: { readOnly?: boolean }
	): DB

	// Create a new database at the given path with the given schema.
	// Use path: null for an in-memory sqlite3 instance
	static createDB(path: string | null, schema: tasl.Schema): DB

	// Import a tasl instance from an AsyncIterable
	static import(
		path: string | null,
		schema: tasl.Schema,
		stream: AsyncIterable<Buffer>
	): Promise<DB>

	// Close the database
	close(): void

	// Get the instance schema
	get schema(): tasl.Schema

	// Export the database to an AsyncIterable
	export(options?: { chunkSize?: number }): AsyncIterable<Buffer>

	// Get an element value
	get(key: string, id: number): tasl.values.Value

	// Iterate over element values
	entries(key: string): Iterable<[number, tasl.values.Value]>

	// Check for the presence of an element with a given id
	has(key: string, id: number): boolean

	// Count the elements of a given class
	count(key: string): number
}
```
