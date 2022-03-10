declare module "node-lmdb" {
	interface OpenEnvOptions {
		path: string
		mapSize: number
		maxDbs: number
	}

	interface KeyOptions {
		keyIsUint32?: boolean
		keyIsBuffer?: boolean
		keyIsString?: boolean
	}

	interface OpenDbiOptions extends KeyOptions {
		name: string | null
		create: boolean
	}

	interface BatchWriteOptions extends KeyOptions {}

	enum BatchWriteResult {
		Success = 0,
		ConditionNotMet = 1,
		KeyNotFound = 2,
	}

	class Env {
		open(options: OpenEnvOptions): void
		close(): void
		openDbi(options: OpenDbiOptions): Dbi
		beginTxn(): Txn
		batchWrite(
			operations: Operation[],
			options: BatchWriteOptions,
			callback: (error: Error | undefined, results: BatchWriteResult[]) => void
		): void
	}

	type Operation = [Dbi, Key] | [Dbi, Key, Buffer]

	class Dbi {
		close(): void
		private constructor()
	}

	type Key = string | number | Buffer

	class Txn {
		private constructor()
		getString(dbi: Dbi, key: Key, options?: KeyOptions): null | string
		putString(dbi: Dbi, key: Key, value: string, options?: KeyOptions): void
		getBinary(dbi: Dbi, key: Key, options?: KeyOptions): null | Buffer
		putBinary(dbi: Dbi, key: Key, value: Buffer, options?: KeyOptions): void
		getNumber(dbi: Dbi, key: Key, options?: KeyOptions): null | number
		putNumber(dbi: Dbi, key: Key, value: number, options?: KeyOptions): void
		getBoolean(dbi: Dbi, key: Key, options?: KeyOptions): null | boolean
		putBoolean(dbi: Dbi, key: Key, value: boolean, options?: KeyOptions): void
		del(dbi: Dbi, key: Key, options?: KeyOptions): void

		abort(): void
		commit(): void
	}

	class Cursor {
		constructor(txn: Txn, dbi: Dbi, options?: KeyOptions)
		goToFirst(): null | Key
		goToNext(): null | Key
		getCurrentString(): string
		getCurrentBinary(): Buffer
		getCurrentNumber(): number
		getCurrentBoolean(): boolean
	}
}
