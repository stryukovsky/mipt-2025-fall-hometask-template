import Decimal from "decimal.js";
const value =
  "0x00010005a005a0027b02f5a20000000000003f90d658b96ababdc5d5dcf9d995";

function parsePrice(slot0Hex: string): string {
  // Remove 0x prefix if present
  const hex = slot0Hex.startsWith("0x") ? slot0Hex.slice(2) : slot0Hex;

  // Pad to 64 characters (32 bytes) if needed
  const paddedHex = hex.padStart(64, "0");

  // Extract sqrtPriceX96 (first 160 bits = 20 bytes = 40 hex chars)
  // Note: We need to read from the right side due to little-endian storage
  const sqrtPriceX96Hex = paddedHex.slice(-40); // Last 40 chars (20 bytes)
  const sqrtPriceX96 = BigInt("0x" + sqrtPriceX96Hex);
  const Q96 = Decimal(2).pow(96);
  // const Q192 = Q96 * Q96;

  const numerator = sqrtPriceX96;

  const price = Decimal(numerator).div(Decimal(Q96));

  return price.pow(2).toString();
}

console.log(parsePrice(value));
