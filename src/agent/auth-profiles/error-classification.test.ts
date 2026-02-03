import { describe, it, expect } from "vitest";
import { classifyError, isRotatableError } from "../runner.js";

// ============================================================
// classifyError
// ============================================================

describe("classifyError", () => {
  it("classifies 401/403/unauthorized as auth", () => {
    expect(classifyError(new Error("HTTP 401 Unauthorized"))).toBe("auth");
    expect(classifyError(new Error("403 Forbidden"))).toBe("auth");
    expect(classifyError(new Error("Invalid API key provided"))).toBe("auth");
    expect(classifyError(new Error("Authentication failed"))).toBe("auth");
  });

  it("classifies 400/malformed as format", () => {
    expect(classifyError(new Error("400 Bad Request"))).toBe("format");
    expect(classifyError(new Error("Invalid request body"))).toBe("format");
    expect(classifyError(new Error("Malformed JSON in request"))).toBe("format");
    expect(classifyError(new Error("Schema validation failed"))).toBe("format");
  });

  it("classifies 429/rate limit as rate_limit", () => {
    expect(classifyError(new Error("429 Too Many Requests"))).toBe("rate_limit");
    expect(classifyError(new Error("Rate limit exceeded"))).toBe("rate_limit");
    expect(classifyError(new Error("rate_limit_error"))).toBe("rate_limit");
  });

  it("classifies billing/quota as billing", () => {
    expect(classifyError(new Error("Billing quota exceeded"))).toBe("billing");
    expect(classifyError(new Error("Insufficient credits"))).toBe("billing");
    expect(classifyError(new Error("Payment required"))).toBe("billing");
  });

  it("classifies timeout/connection errors as timeout", () => {
    expect(classifyError(new Error("Request timed out"))).toBe("timeout");
    expect(classifyError(new Error("ETIMEDOUT"))).toBe("timeout");
    expect(classifyError(new Error("ECONNRESET"))).toBe("timeout");
    expect(classifyError(new Error("Connection timeout"))).toBe("timeout");
  });

  it("classifies unknown errors as unknown", () => {
    expect(classifyError(new Error("Something went wrong"))).toBe("unknown");
    expect(classifyError("string error")).toBe("unknown");
    expect(classifyError(42)).toBe("unknown");
  });
});

// ============================================================
// isRotatableError
// ============================================================

describe("isRotatableError", () => {
  it("considers auth, rate_limit, billing, timeout as rotatable", () => {
    expect(isRotatableError("auth")).toBe(true);
    expect(isRotatableError("rate_limit")).toBe(true);
    expect(isRotatableError("billing")).toBe(true);
    expect(isRotatableError("timeout")).toBe(true);
  });

  it("does not rotate on format or unknown errors", () => {
    expect(isRotatableError("format")).toBe(false);
    expect(isRotatableError("unknown")).toBe(false);
  });
});
