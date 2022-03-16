import { types, values, expressions, forComponents, Mapping } from "tasl"

import { DB } from "./db.js"

export function makeMigrationQuery(mapping: Mapping): string {
	for (const [key] of mapping.entries()) {
	}
	return ""
}

export async function applyExpression(
	db: DB,
	expression: expressions.Expression,
	targetType: types.Type,
	environment: Record<string, [types.Type, values.Value]>
): Promise<values.Value> {
	if (expression.kind === "uri") {
		if (targetType.kind !== "uri") {
			throw new Error("unexpected URI expression")
		}

		return values.uri(expression.value)
	} else if (expression.kind === "literal") {
		if (targetType.kind !== "literal") {
			throw new Error("unexpected URI expression")
		}

		return values.literal(expression.value)
	} else if (expression.kind === "product") {
		if (targetType.kind !== "product") {
			throw new Error("unexpected construction expression")
		}

		const components: Record<string, values.Value> = {}
		for (const [key, target] of forComponents(targetType)) {
			const source = expression.components[key]
			if (source === undefined) {
				throw new Error(`missing component ${key}`)
			}

			components[key] = await applyExpression(db, source, target, environment)
		}

		return values.product(components)
	} else if (expression.kind === "term") {
		const { id, path } = expression
		const [t, v] = await evaluateTerm(db, id, path, environment)
		return project([t, v], targetType)
	} else if (expression.kind === "match") {
		const { id, path, cases } = expression
		const [t, v] = await evaluateTerm(db, id, path, environment)
		if (t.kind !== "coproduct") {
			throw new Error(
				"the term value of a match expression must be a coproduct"
			)
		} else if (v.kind !== "coproduct") {
			throw new Error("internal type error")
		} else if (t.options[v.key] === undefined) {
			throw new Error("internal type error")
		}

		if (cases[v.key] === undefined) {
			throw new Error(`missing case for option ${v.key}`)
		}

		return await applyExpression(db, cases[v.key].value, targetType, {
			...environment,
			[cases[v.key].id]: [t.options[v.key], v.value],
		})
	} else if (expression.kind === "coproduct") {
		if (targetType.kind !== "coproduct") {
			throw new Error("unexpected injection expression")
		}

		const option = targetType.options[expression.key]
		if (option === undefined) {
			throw new Error(`injection key ${expression.key} is not an option`)
		}

		return values.coproduct(
			expression.key,
			await applyExpression(db, expression.value, option, environment)
		)
	} else {
		throw new Error("invalid expression")
	}
}

export async function evaluateTerm(
	db: DB,
	id: string,
	path: (expressions.Projection | expressions.Dereference)[],
	environment: Record<string, [types.Type, values.Value]>
): Promise<[types.Type, values.Value]> {
	return path.reduce<Promise<[types.Type, values.Value]>>(
		async (term, segment) => {
			const [t, v] = await term
			if (segment.kind === "projection") {
				if (t.kind !== "product") {
					throw new Error("invalid projection: term value is not a product")
				} else if (v.kind !== "product") {
					throw new Error("internal type error")
				}

				if (t.components[segment.key] === undefined) {
					throw new Error(
						`invalid projection: no component with key ${segment.key}`
					)
				} else if (v.components[segment.key] === undefined) {
					throw new Error("internal type error")
				}

				return [t.components[segment.key], v.components[segment.key]]
			} else if (segment.kind === "dereference") {
				if (t.kind !== "reference") {
					throw new Error("invalid dereference: term value is not a reference")
				} else if (v.kind !== "reference") {
					throw new Error("internal type error")
				}

				const type = db.schema.get(segment.key)
				const value = await db.get(segment.key, v.id)
				return [type, value]
			} else {
				throw new Error("invalid path segment")
			}
		},
		Promise.resolve(environment[id])
	)
}

export function project(
	[t, v]: [types.Type, values.Value],
	targetType: types.Type
): values.Value {
	if (targetType.kind === "uri") {
		if (t.kind !== "uri") {
			throw new Error(
				`invalid type - expected a URI value, but got a ${t.kind}`
			)
		} else if (v.kind !== "uri") {
			throw new Error("internal type error")
		}

		return v
	} else if (targetType.kind === "literal") {
		if (t.kind !== "literal") {
			throw new Error(
				`invalid type - expected a literal value, but got a ${t.kind}`
			)
		} else if (v.kind !== "literal") {
			throw new Error("internal type error")
		}

		if (targetType.datatype !== t.datatype) {
			throw new Error(
				`invalid type - expected a literal with datatype ${targetType.datatype}, but got a literal with datatype ${t.datatype}`
			)
		}

		return v
	} else if (targetType.kind === "product") {
		if (t.kind !== "product") {
			throw new Error(
				`invalid type - expected a product value, but got a ${t.kind}`
			)
		} else if (v.kind !== "product") {
			throw new Error("internal type error")
		}

		const components: Record<string, values.Value> = {}
		for (const [key, component] of forComponents(targetType)) {
			if (t.components[key] === undefined) {
				throw new Error(`invalid type - missing component ${key}`)
			}

			components[key] = project(
				[t.components[key], v.components[key]],
				component
			)
		}

		return values.product(components)
	} else if (targetType.kind === "coproduct") {
		if (t.kind !== "coproduct") {
			throw new Error(
				`invalid type - expected a coproduct value, but got a ${t.kind}`
			)
		} else if (v.kind !== "coproduct") {
			throw new Error("internal type error")
		}

		if (targetType.options[v.key] === undefined) {
			throw new Error(`invalid type - ${v.key} is not an option`)
		}

		return values.coproduct(
			v.key,
			project([t.options[v.key], v.value], targetType.options[v.key])
		)
	} else if (targetType.kind === "reference") {
		if (t.kind !== "reference") {
			throw new Error(
				`invalid type - expected a reference value, but got a ${t.kind}`
			)
		} else if (v.kind !== "reference") {
			throw new Error("internal type error")
		}

		return values.reference(v.id)
	} else {
		throw new Error("invalid type")
	}
}
