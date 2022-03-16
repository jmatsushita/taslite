import { xsd } from "@underlay/namespaces"

export type Values = Record<string, number | string | Buffer | null>

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
