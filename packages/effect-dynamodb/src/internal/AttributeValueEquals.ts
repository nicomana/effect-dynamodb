/**
 * Structural equality of marshalled DynamoDB attribute values.
 *
 * Lives in its own module so both `Entity.ts` and `internal/TransactPlan.ts`
 * can use it without a runtime import cycle.
 */

import type { AttributeValue } from "@aws-sdk/client-dynamodb"

const bytesKey = (bytes: Uint8Array): string => Array.from(bytes).join(",")

/** Structural equality of two marshalled values (sets compare as sets). */
export const attributeValueEquals = (
  a: AttributeValue | undefined,
  b: AttributeValue | undefined,
): boolean => {
  if (a === undefined || b === undefined) return a === b
  const [kind] = Object.keys(a)
  if (kind === undefined || Object.keys(b)[0] !== kind) return false
  const x = (a as unknown as globalThis.Record<string, unknown>)[kind]
  const y = (b as unknown as globalThis.Record<string, unknown>)[kind]
  switch (kind) {
    case "L": {
      const xs = x as ReadonlyArray<AttributeValue>
      const ys = y as ReadonlyArray<AttributeValue>
      return xs.length === ys.length && xs.every((v, i) => attributeValueEquals(v, ys[i]))
    }
    case "M": {
      const xm = x as globalThis.Record<string, AttributeValue>
      const ym = y as globalThis.Record<string, AttributeValue>
      const keys = Object.keys(xm)
      return (
        keys.length === Object.keys(ym).length &&
        keys.every((k) => attributeValueEquals(xm[k], ym[k]))
      )
    }
    case "SS":
    case "NS": {
      const xs = new Set(x as ReadonlyArray<string>)
      const ys = y as ReadonlyArray<string>
      return xs.size === new Set(ys).size && ys.every((v) => xs.has(v))
    }
    case "B":
      return bytesKey(x as Uint8Array) === bytesKey(y as Uint8Array)
    case "BS": {
      const xs = new Set((x as ReadonlyArray<Uint8Array>).map(bytesKey))
      const ys = (y as ReadonlyArray<Uint8Array>).map(bytesKey)
      return xs.size === new Set(ys).size && ys.every((v) => xs.has(v))
    }
    default:
      return x === y
  }
}
