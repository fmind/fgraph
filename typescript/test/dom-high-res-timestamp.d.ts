// tinybench, which vitest 5 bundles, types its timings with the DOM-only
// DOMHighResTimeStamp alias. Declaring that one alias keeps library type checks
// on (skipLibCheck: false) without adding the whole DOM lib to a Node package.
declare global {
  type DOMHighResTimeStamp = number;
}

export {};
