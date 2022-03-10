import * as tasl from "tasl"
import { signalInvalidType } from "tasl/lib/utils.js"

type Leaf = number
type Index = Leaf | Array<Leaf> | { index: Leaf; value: Leaf }

function buildIndex(type: tasl.types.Type, data: Buffer): Index {
	if (type.kind === "uri") {
		return 0
	} else if (type.kind === "literal") {
		return 0
	} else if (type.kind === "product") {
		return 0
	} else if (type.kind === "coproduct") {
		return 0
	} else if (type.kind === "reference") {
		return 0
	} else {
		signalInvalidType(type)
	}
}

function resolveIndex(
	type: tasl.types.Type,
	data: Buffer,
	index: Index,
	path: number[]
): [null, null] | [tasl.types.Type, tasl.values.Value] {
  
	return [null, null]
}

/**
 * URI:
 *   - length, offset (worth decoding??)
 * Literals:
 *   - if fixed-size: nothing
 *   - if variable-length: length, offset (???)
 * Products:
 *   - byte offsets for the start of each component
 * Coproducts:
 *   - option index, offset (???)
 * References:
 *   - index
 */
