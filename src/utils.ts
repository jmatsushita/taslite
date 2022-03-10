import sqlite3 from "sqlite3"

export function elementTableName(index: number) {
	return `c${index}`
}

export type Values = Record<string, number | string | Uint8Array>

export function finalize(statement: sqlite3.Statement): Promise<void> {
	return new Promise((resolve, reject) =>
		statement.finalize((err) => (err ? reject(err) : resolve()))
	)
}

export function runStatement(
	statement: sqlite3.Statement,
	params: Values = {}
): Promise<void> {
	return new Promise((resolve, reject) =>
		statement.run(params, (err) => (err ? reject(err) : resolve()))
	)
}

export function getStatement(
	statement: sqlite3.Statement,
	params: Values = {}
): Promise<Values> {
	return new Promise((resolve, reject) => {
		statement.get(params, (err, row) => (err ? reject(err) : resolve(row)))
	})
}

export function runQuery(
	db: sqlite3.Database,
	sql: string,
	params: Values = {}
): Promise<void> {
	return new Promise((resolve, reject) =>
		db.run(sql, params, (err) => (err ? reject(err) : resolve()))
	)
}

export function getQuery(
	db: sqlite3.Database,
	sql: string,
	params: Values = {}
): Promise<Values> {
	return new Promise((resolve, reject) =>
		db.get(sql, params, (err, row) => (err ? reject(err) : resolve(row)))
	)
}
