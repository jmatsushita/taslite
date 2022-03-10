import * as tasl from "tasl"

export const microSchema = new tasl.Schema({
	"http://example.com/a": tasl.types.product({
		"http://example.com/a/a": tasl.types.u8,
		"http://example.com/a/b": tasl.types.boolean,
	}),
	"http://example.com/b": tasl.types.coproduct({
		"http://example.com/b/a": tasl.types.bytes,
		"http://example.com/b/b": tasl.types.unit,
		"http://example.com/b/c": tasl.types.uri(),
	}),
})

export const microInstance = new tasl.Instance(microSchema, {
	"http://example.com/a": [
		{
			id: 0,
			value: tasl.values.product({
				"http://example.com/a/a": tasl.values.u8(0xff),
				"http://example.com/a/b": tasl.values.boolean(false),
			}),
		},
	],
	"http://example.com/b": [
		{
			id: 0,
			value: tasl.values.coproduct(
				"http://example.com/b/a",
				tasl.values.bytes(new Uint8Array([0x0f, 0xee, 0x12, 0x00]))
			),
		},
		{
			id: 1,
			value: tasl.values.coproduct(
				"http://example.com/b/b",
				tasl.values.unit()
			),
		},
		{
			id: 2,
			value: tasl.values.coproduct(
				"http://example.com/b/b",
				tasl.values.unit()
			),
		},
		{
			id: 3,
			value: tasl.values.coproduct(
				"http://example.com/b/c",
				tasl.values.uri(
					"dweb:/ipfs/bafybeiczsscdsbs7ffqz55asqdf3smv6klcw3gofszvwlyarci47bgf354"
				)
			),
		},
	],
})
