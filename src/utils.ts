import { xsd } from "@underlay/namespaces"
import * as tasl from "tasl"
import { floatToString } from "tasl/lib/utils.js"
import { parseFloat } from "tasl/lib/instance/literals/validateLiteral.js"

export type Values = Record<string, string | number | Buffer | null>

export function getTableName(classIndex: number) {
	return `c${classIndex}`
}

export function getPropertyName(path: number[]) {
	if (path.length === 0) {
		return "e"
	} else {
		return `e_${path.join("_")}`
	}
}

export const fixedSizeLiterals: Record<string, number> = {
	[xsd.boolean]: 1,
	[xsd.double]: 8,
	[xsd.float]: 4,
	[xsd.long]: 8,
	[xsd.int]: 4,
	[xsd.short]: 2,
	[xsd.byte]: 1,
	[xsd.unsignedLong]: 8,
	[xsd.unsignedInt]: 4,
	[xsd.unsignedShort]: 2,
	[xsd.unsignedByte]: 1,
}

export const integerDatatypes = new Set<string>([
	xsd.boolean,
	xsd.long,
	xsd.int,
	xsd.short,
	xsd.byte,
	xsd.unsignedLong,
	xsd.unsignedInt,
	xsd.unsignedShort,
	xsd.unsignedByte,
])

export function getPropertyType(
	schema: tasl.Schema,
	property: tasl.types.Type
): string {
	if (property.kind === "uri") {
		return "TEXT"
	} else if (property.kind === "literal") {
		if (integerDatatypes.has(property.datatype)) {
			return "INTEGER"
		} else if (
			property.datatype === xsd.double ||
			property.datatype === xsd.float
		) {
			return "REAL"
		} else if (property.datatype === xsd.hexBinary) {
			return "BLOB"
		} else {
			return "TEXT"
		}
	} else if (property.kind === "coproduct") {
		return "INTEGER"
	} else if (property.kind === "reference") {
		const index = schema.indexOfKey(property.key)
		const name = getTableName(index)
		return `INTEGER REFERENCES ${name}(id)`
	} else {
		throw new Error("invalid property type")
	}
}

export function* getProperties(
	type: tasl.types.Type,
	path: number[] = [],
	optional = false
): Iterable<[number[], Exclude<tasl.types.Type, tasl.types.Product>, boolean]> {
	if (type.kind === "uri") {
		yield [path, type, optional]
	} else if (type.kind === "literal") {
		yield [path, type, optional]
	} else if (type.kind === "product") {
		for (const [_, component, index] of tasl.forComponents(type)) {
			yield* getProperties(component, [...path, index], optional)
		}
	} else if (type.kind === "coproduct") {
		yield [path, type, optional]
		for (const [_, option, index] of tasl.forOptions(type)) {
			yield* getProperties(option, [...path, index], true)
		}
	} else if (type.kind === "reference") {
		yield [path, type, optional]
	} else {
		throw new Error("invalid type")
	}
}

export function* getPropertyValues(
	type: tasl.types.Type,
	value: tasl.values.Value,
	path: number[] = []
): Iterable<
	[
		number[],
		Exclude<tasl.types.Type, tasl.types.Product>,
		null | Exclude<tasl.values.Value, tasl.values.Product>
	]
> {
	if (type.kind === "uri" && value.kind === "uri") {
		yield [path, type, value]
	} else if (type.kind === "literal" && value.kind === "literal") {
		yield [path, type, value]
	} else if (type.kind === "product" && value.kind === "product") {
		for (const [key, component, index] of tasl.forComponents(type)) {
			const componentPath = [...path, index]
			yield* getPropertyValues(component, value.components[key], componentPath)
		}
	} else if (type.kind === "coproduct" && value.kind === "coproduct") {
		yield [path, type, value]
		for (const [key, option, index] of tasl.forOptions(type)) {
			const optionPath = [...path, index]
			if (key === value.key) {
				yield* getPropertyValues(option, value.value, optionPath)
			} else {
				for (const [path, type] of getProperties(option, optionPath)) {
					yield [path, type, null]
				}
			}
		}
	} else if (type.kind === "reference" && value.kind === "reference") {
		yield [path, type, value]
	} else {
		throw new Error("internal type error")
	}
}

export function parseValue(
	type: tasl.types.Type,
	path: number[],
	row: Values
): tasl.values.Value {
	const name = getPropertyName(path)

	if (type.kind === "uri") {
		const value = row[name]
		if (typeof value !== "string") {
			throw new Error(`internal error parsing value: invalid property ${name}`)
		}

		return tasl.values.uri(value)
	} else if (type.kind === "literal") {
		const value = parseLiteralValue(type.datatype, row[name])
		return tasl.values.literal(value)
	} else if (type.kind === "product") {
		const components: Record<string, tasl.values.Value> = {}
		for (const [key, component, index] of tasl.forComponents(type)) {
			components[key] = parseValue(component, [...path, index], row)
		}
		return tasl.values.product(components)
	} else if (type.kind === "coproduct") {
		const name = getPropertyName(path)
		const index = row[name]

		if (typeof index !== "number") {
			throw new Error(`internal error parsing value: invalid property ${name}`)
		}

		const [key, option] = tasl.optionAtIndex(type, index)
		return tasl.values.coproduct(key, parseValue(option, [...path, index], row))
	} else if (type.kind === "reference") {
		const name = getPropertyName(path)
		const value = row[name]
		if (typeof value !== "number") {
			throw new Error(`internal error parsing value: invalid property ${name}`)
		}
		return tasl.values.reference(value)
	} else {
		throw new Error("invalid type")
	}
}

function parseLiteralValue(
	datatype: string,
	value: string | number | Buffer | null
): string {
	if (integerDatatypes.has(datatype)) {
		if (typeof value !== "number") {
			throw new Error(`internal error parsing property value`)
		}

		if (datatype === xsd.boolean) {
			if (value === 0) {
				return "false"
			} else if (value === 1) {
				return "true"
			} else {
				throw new Error("interal error: invalid boolean value")
			}
		} else {
			return value.toString()
		}
	} else if (datatype === xsd.double || datatype === xsd.float) {
		if (typeof value !== "number") {
			throw new Error(`internal error parsing property value`)
		}

		return floatToString(value)
	} else if (datatype === xsd.hexBinary) {
		if (!Buffer.isBuffer(value)) {
			throw new Error(`internal error parsing property value`)
		}

		return value.toString("hex")
	} else {
		if (typeof value !== "string") {
			throw new Error(`internal error parsing property value`)
		}

		return value
	}
}

export function serializeValue(
	type: tasl.types.Type,
	value: tasl.values.Value
): Values {
	const params: Values = {}
	for (const [path, t, v] of getPropertyValues(type, value, [])) {
		const name = getPropertyName(path)
		if (v === null) {
			params[name] = null
		} else if (t.kind === "uri" && v.kind === "uri") {
			params[name] = v.value
		} else if (t.kind === "literal" && v.kind === "literal") {
			params[name] = serializeLiteralValue(t.datatype, v.value)
		} else if (t.kind === "coproduct" && v.kind === "coproduct") {
			const index = tasl.indexOfOption(t, v.key)
			params[name] = index
		} else if (t.kind === "reference" && v.kind === "reference") {
			params[name] = v.id
		} else {
			throw new Error("internal type error")
		}
	}
	return params
}

function serializeLiteralValue(
	datatype: string,
	value: string
): string | number | Buffer {
	if (integerDatatypes.has(datatype)) {
		if (datatype === xsd.boolean) {
			if (value === "true" || value === "1") {
				return 1
			} else if (value === "false" || value === "0") {
				return 0
			} else {
				throw new Error("invalid boolean value")
			}
		} else {
			const i = parseInt(value)
			if (Number.isSafeInteger(i)) {
				return i
			} else {
				throw new Error(`invalid integer ${value}`)
			}
		}
	} else if (datatype === xsd.float || datatype === xsd.double) {
		return parseFloat(value)
	} else if (datatype === xsd.hexBinary) {
		return Buffer.from(value, "hex")
	} else {
		return value
	}
}
