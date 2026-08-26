/** Base class for every public fgraph error. */
export class FGraphError extends Error {
  override readonly name: string;

  constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

export class NotFound extends FGraphError {}
export class Conflict extends FGraphError {}
export class SchemaError extends FGraphError {}
export class TypeError extends FGraphError {}
export class QueryError extends FGraphError {}
export class FormatError extends FGraphError {}
export class ReadOnly extends FGraphError {}
export class TooLarge extends FGraphError {}
export class Unsupported extends FGraphError {}

export function errorName(error: unknown): string {
  return error instanceof FGraphError ? error.name : "Error";
}
