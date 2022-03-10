import * as tasl from "tasl"

export const nanoSchema = new tasl.Schema({
	"http://example.com/foo": tasl.types.boolean,
})

export const nanoInstance = new tasl.Instance(nanoSchema, {
	"http://example.com/foo": [
		{ id: 0, value: tasl.values.boolean(true) },
		{ id: 1, value: tasl.values.boolean(false) },
		{ id: 2, value: tasl.values.boolean(true) },
	],
})
