#!/usr/bin/env node
// Quarry Executor Bundle v0.13.6
// This is a bundled version for embedding in the quarry binary.
// Do not edit directly - regenerate with: task executor:bundle

var __create = Object.create;
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getProtoOf = Object.getPrototypeOf;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __esm = (fn, res, err) => function __init() {
  if (err) throw err[0];
  try {
    return fn && (res = (0, fn[__getOwnPropNames(fn)[0]])(fn = 0)), res;
  } catch (e) {
    throw err = [e], e;
  }
};
var __commonJS = (cb, mod) => function __require() {
  try {
    return mod || (0, cb[__getOwnPropNames(cb)[0]])((mod = { exports: {} }).exports, mod), mod.exports;
  } catch (e) {
    throw mod = 0, e;
  }
};
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(
  // If the importer is in node compatibility mode or this is not an ESM
  // file that has been converted to a CommonJS file using a Babel-
  // compatible transform (i.e. "__esModule" has not been set), then set
  // "default" to the CommonJS "module.exports" for node compatibility.
  isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: true }) : target,
  mod
));

// ../sdk/dist/index.mjs
import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import v8 from "node:v8";
function buildStorageKey(partition, filename) {
  return `datasets/${partition.dataset}/partitions/source=${partition.source}/category=${partition.category}/day=${partition.day}/run_id=${partition.run_id}/files/${filename}`;
}
function createAPIs(run, sink, storagePartition) {
  let seq = 0;
  let terminalEmitted = false;
  let sinkFailed = null;
  let chain = Promise.resolve();
  function serialize(fn) {
    const result = chain.then(async () => {
      if (sinkFailed !== null) throw new SinkFailedError(sinkFailed);
      try {
        return await fn();
      } catch (err) {
        sinkFailed = err;
        throw err;
      }
    });
    chain = result.then(() => {
    }, () => {
    });
    return result;
  }
  function createEnvelope(type, payload) {
    return {
      contract_version: CONTRACT_VERSION,
      event_id: randomUUID(),
      run_id: run.run_id,
      type,
      ts: (/* @__PURE__ */ new Date()).toISOString(),
      payload,
      attempt: run.attempt,
      ...run.job_id !== void 0 && { job_id: run.job_id },
      ...run.parent_run_id !== void 0 && { parent_run_id: run.parent_run_id }
    };
  }
  async function writeEnvelope(envelope) {
    seq += 1;
    const complete = {
      ...envelope,
      seq
    };
    await sink.writeEvent(complete);
  }
  function assertNotTerminal() {
    if (terminalEmitted) throw new TerminalEventError();
  }
  function emitEvent(type, payload) {
    return serialize(async () => {
      assertNotTerminal();
      await writeEnvelope(createEnvelope(type, payload));
    });
  }
  const emit = {
    item(options) {
      return emitEvent("item", {
        item_type: options.item_type,
        data: options.data
      });
    },
    artifact(options) {
      return serialize(async () => {
        assertNotTerminal();
        const artifact_id = randomUUID();
        const size_bytes = options.data.byteLength;
        await sink.writeArtifactData(artifact_id, options.data);
        await writeEnvelope(createEnvelope("artifact", {
          artifact_id,
          name: options.name,
          content_type: options.content_type,
          size_bytes
        }));
        return artifact_id;
      });
    },
    checkpoint(options) {
      return emitEvent("checkpoint", {
        checkpoint_id: options.checkpoint_id,
        ...options.note !== void 0 && { note: options.note }
      });
    },
    enqueue(options) {
      return emitEvent("enqueue", {
        target: options.target,
        params: options.params,
        ...options.source !== void 0 && { source: options.source },
        ...options.category !== void 0 && { category: options.category }
      });
    },
    rotateProxy(options) {
      return emitEvent("rotate_proxy", { ...options?.reason !== void 0 && { reason: options.reason } });
    },
    log(options) {
      return emitEvent("log", {
        level: options.level,
        message: options.message,
        ...options.fields !== void 0 && { fields: options.fields }
      });
    },
    async debug(message, fields) {
      await emit.log({
        level: "debug",
        message,
        fields
      });
    },
    async info(message, fields) {
      await emit.log({
        level: "info",
        message,
        fields
      });
    },
    async warn(message, fields) {
      await emit.log({
        level: "warn",
        message,
        fields
      });
    },
    async error(message, fields) {
      await emit.log({
        level: "error",
        message,
        fields
      });
    },
    runError(options) {
      return serialize(async () => {
        assertNotTerminal();
        await writeEnvelope(createEnvelope("run_error", {
          error_type: options.error_type,
          message: options.message,
          ...options.stack !== void 0 && { stack: options.stack }
        }));
        terminalEmitted = true;
      });
    },
    runComplete(options) {
      return serialize(async () => {
        assertNotTerminal();
        await writeEnvelope(createEnvelope("run_complete", { ...options?.summary !== void 0 && { summary: options.summary } }));
        terminalEmitted = true;
      });
    }
  };
  function validateFilename(filename) {
    if (!filename) throw new StorageFilenameError(filename, "filename must not be empty");
    if (filename.includes("/") || filename.includes("\\")) throw new StorageFilenameError(filename, "filename must not contain path separators");
    if (filename.includes("..")) throw new StorageFilenameError(filename, 'filename must not contain ".."');
  }
  return {
    emit,
    storage: { put(options) {
      return serialize(async () => {
        assertNotTerminal();
        validateFilename(options.filename);
        await sink.writeFile(options.filename, options.content_type, options.data);
        return { key: storagePartition ? buildStorageKey(storagePartition, options.filename) : "" };
      });
    } }
  };
}
function validateThresholds(t) {
  for (const [name, value] of Object.entries(t)) if (value <= 0 || value > 1) throw new RangeError(`MemoryThreshold "${name}" must be in (0, 1], got ${value}`);
  if (t.moderate >= t.high) throw new RangeError(`MemoryThresholds must be monotonic: moderate (${t.moderate}) must be < high (${t.high})`);
  if (t.high >= t.critical) throw new RangeError(`MemoryThresholds must be monotonic: high (${t.high}) must be < critical (${t.critical})`);
}
function classifyPressure(ratio, thresholds) {
  if (ratio >= thresholds.critical) return "critical";
  if (ratio >= thresholds.high) return "high";
  if (ratio >= thresholds.moderate) return "moderate";
  return "low";
}
function highestPressure(levels) {
  let max = 0;
  for (const level of levels) {
    const idx = PRESSURE_ORDER.indexOf(level);
    if (idx > max) max = idx;
  }
  return PRESSURE_ORDER[max];
}
function readNodeUsage() {
  const heapStats = v8.getHeapStatistics();
  const used = process.memoryUsage().heapUsed;
  const limit = heapStats.heap_size_limit;
  return {
    used,
    limit,
    ratio: limit > 0 ? used / limit : 0
  };
}
async function readBrowserUsage(page) {
  if (!page) return null;
  try {
    const m = await page.metrics();
    const used = m.JSHeapUsedSize ?? 0;
    const limit = m.JSHeapTotalSize ?? 0;
    return {
      used,
      limit,
      ratio: limit > 0 ? used / limit : 0
    };
  } catch {
    return null;
  }
}
function readCgroupUsage(readFile = readCgroupFile) {
  const v2Current = readFile("/sys/fs/cgroup/memory.current");
  const v2Max = readFile("/sys/fs/cgroup/memory.max");
  if (v2Current !== null && v2Max !== null) return {
    used: v2Current,
    limit: v2Max,
    ratio: v2Max > 0 ? v2Current / v2Max : 0
  };
  const v1Usage = readFile("/sys/fs/cgroup/memory/memory.usage_in_bytes");
  const v1Limit = readFile("/sys/fs/cgroup/memory/memory.limit_in_bytes");
  if (v1Usage !== null && v1Limit !== null) {
    if (v1Limit >= CGROUP_UNLIMITED_THRESHOLD) return null;
    return {
      used: v1Usage,
      limit: v1Limit,
      ratio: v1Limit > 0 ? v1Usage / v1Limit : 0
    };
  }
  return null;
}
function readCgroupFile(path) {
  try {
    const content = readFileSync(path, "utf8").trim();
    if (content === "max") return null;
    const value = Number.parseInt(content, 10);
    return Number.isNaN(value) ? null : value;
  } catch {
    return null;
  }
}
function createMemoryAPI(options) {
  const thresholds = {
    ...DEFAULT_THRESHOLDS,
    ...options.thresholds
  };
  validateThresholds(thresholds);
  const readNode = options._readNode ?? readNodeUsage;
  const readBrowser = options._readBrowser ?? readBrowserUsage;
  const readCgroup = options._readCgroup ?? (() => readCgroupUsage(options._readCgroupFile));
  async function snapshot(opts) {
    const includeBrowser = opts?.browser !== false;
    const node = readNode();
    const browser = includeBrowser ? await readBrowser(options.page) : null;
    const cgroup = readCgroup();
    const levels = [classifyPressure(node.ratio, thresholds)];
    if (browser) levels.push(classifyPressure(browser.ratio, thresholds));
    if (cgroup) levels.push(classifyPressure(cgroup.ratio, thresholds));
    return {
      node,
      browser,
      cgroup,
      pressure: highestPressure(levels),
      ts: (/* @__PURE__ */ new Date()).toISOString()
    };
  }
  return {
    snapshot,
    async isAbove(level) {
      const snap = await snapshot();
      return PRESSURE_ORDER.indexOf(snap.pressure) >= PRESSURE_ORDER.indexOf(level);
    }
  };
}
function createContext(options) {
  const { emit, storage } = createAPIs(options.run, options.sink, options.storagePartition);
  const memory = createMemoryAPI({
    page: options.page,
    thresholds: options.memoryThresholds
  });
  const ctx = {
    job: options.job,
    run: Object.freeze(options.run),
    page: options.page,
    browser: options.browser,
    browserContext: options.browserContext,
    emit,
    storage,
    memory
  };
  return Object.freeze(ctx);
}
var CONTRACT_VERSION, TerminalEventError, SinkFailedError, StorageFilenameError, PRESSURE_ORDER, DEFAULT_THRESHOLDS, CGROUP_UNLIMITED_THRESHOLD;
var init_dist = __esm({
  "../sdk/dist/index.mjs"() {
    "use strict";
    CONTRACT_VERSION = "0.13.6";
    TerminalEventError = class extends Error {
      constructor() {
        super("Cannot emit: a terminal event (run_error or run_complete) has already been emitted");
        this.name = "TerminalEventError";
      }
    };
    SinkFailedError = class extends Error {
      constructor(cause) {
        super("Cannot emit: sink has previously failed");
        this.name = "SinkFailedError";
        this.cause = cause;
      }
    };
    StorageFilenameError = class extends Error {
      constructor(filename, reason) {
        super(`Invalid storage filename "${filename}": ${reason}`);
        this.name = "StorageFilenameError";
      }
    };
    PRESSURE_ORDER = [
      "low",
      "moderate",
      "high",
      "critical"
    ];
    DEFAULT_THRESHOLDS = {
      moderate: 0.5,
      high: 0.7,
      critical: 0.9
    };
    CGROUP_UNLIMITED_THRESHOLD = 2 ** 62;
  }
});

// src/ipc/observing-sink.ts
function isTerminalType(type) {
  return type === "run_complete" || type === "run_error";
}
function isPlainObject(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
var SinkAlreadyFailedError, ObservingSink;
var init_observing_sink = __esm({
  "src/ipc/observing-sink.ts"() {
    "use strict";
    SinkAlreadyFailedError = class extends Error {
      constructor(originalCause) {
        const causeMsg = originalCause instanceof Error ? originalCause.message : String(originalCause);
        super(`Sink has already failed: ${causeMsg}`);
        this.name = "SinkAlreadyFailedError";
        this.cause = originalCause;
      }
    };
    ObservingSink = class {
      constructor(inner) {
        this.inner = inner;
      }
      inner;
      terminalState = null;
      sinkFailure = null;
      /**
       * Write an event envelope, tracking the first terminal event on success.
       * @throws SinkAlreadyFailedError if the sink has previously failed
       */
      async writeEvent(envelope) {
        if (this.sinkFailure !== null) {
          throw new SinkAlreadyFailedError(this.sinkFailure);
        }
        try {
          await this.inner.writeEvent(envelope);
          if (this.terminalState === null && isTerminalType(envelope.type)) {
            this.terminalState = this.extractTerminalState(envelope.type, envelope.payload);
          }
        } catch (err) {
          if (this.sinkFailure === null) {
            this.sinkFailure = err;
          }
          throw err;
        }
      }
      /**
       * Write artifact data, tracking failures.
       * @throws SinkAlreadyFailedError if the sink has previously failed
       */
      async writeArtifactData(artifactId, data) {
        if (this.sinkFailure !== null) {
          throw new SinkAlreadyFailedError(this.sinkFailure);
        }
        try {
          await this.inner.writeArtifactData(artifactId, data);
        } catch (err) {
          if (this.sinkFailure === null) {
            this.sinkFailure = err;
          }
          throw err;
        }
      }
      /**
       * Write a sidecar file, tracking failures.
       * @throws SinkAlreadyFailedError if the sink has previously failed
       */
      async writeFile(filename, contentType, data) {
        if (this.sinkFailure !== null) {
          throw new SinkAlreadyFailedError(this.sinkFailure);
        }
        try {
          await this.inner.writeFile(filename, contentType, data);
        } catch (err) {
          if (this.sinkFailure === null) {
            this.sinkFailure = err;
          }
          throw err;
        }
      }
      /**
       * Extract terminal state from type and payload.
       * Type alone is authoritative; payload fields are best-effort.
       */
      extractTerminalState(type, payload) {
        if (type === "run_error") {
          let errorType;
          let message;
          if (isPlainObject(payload)) {
            if ("error_type" in payload && typeof payload.error_type === "string") {
              errorType = payload.error_type;
            }
            if ("message" in payload && typeof payload.message === "string") {
              message = payload.message;
            }
          }
          return { type: "run_error", errorType, message };
        }
        let summary;
        if (isPlainObject(payload) && "summary" in payload && isPlainObject(payload.summary)) {
          summary = payload.summary;
        }
        return { type: "run_complete", summary };
      }
      // SinkState implementation
      getTerminalState() {
        return this.terminalState;
      }
      isSinkFailed() {
        return this.sinkFailure !== null;
      }
      getSinkFailure() {
        return this.sinkFailure;
      }
    };
  }
});

// ../node_modules/.pnpm/@msgpack+msgpack@3.1.3/node_modules/@msgpack/msgpack/dist.cjs/utils/utf8.cjs
var require_utf8 = __commonJS({
  "../node_modules/.pnpm/@msgpack+msgpack@3.1.3/node_modules/@msgpack/msgpack/dist.cjs/utils/utf8.cjs"(exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.utf8Count = utf8Count;
    exports.utf8EncodeJs = utf8EncodeJs;
    exports.utf8EncodeTE = utf8EncodeTE;
    exports.utf8Encode = utf8Encode;
    exports.utf8DecodeJs = utf8DecodeJs;
    exports.utf8DecodeTD = utf8DecodeTD;
    exports.utf8Decode = utf8Decode;
    function utf8Count(str) {
      const strLength = str.length;
      let byteLength = 0;
      let pos = 0;
      while (pos < strLength) {
        let value = str.charCodeAt(pos++);
        if ((value & 4294967168) === 0) {
          byteLength++;
          continue;
        } else if ((value & 4294965248) === 0) {
          byteLength += 2;
        } else {
          if (value >= 55296 && value <= 56319) {
            if (pos < strLength) {
              const extra = str.charCodeAt(pos);
              if ((extra & 64512) === 56320) {
                ++pos;
                value = ((value & 1023) << 10) + (extra & 1023) + 65536;
              }
            }
          }
          if ((value & 4294901760) === 0) {
            byteLength += 3;
          } else {
            byteLength += 4;
          }
        }
      }
      return byteLength;
    }
    function utf8EncodeJs(str, output, outputOffset) {
      const strLength = str.length;
      let offset = outputOffset;
      let pos = 0;
      while (pos < strLength) {
        let value = str.charCodeAt(pos++);
        if ((value & 4294967168) === 0) {
          output[offset++] = value;
          continue;
        } else if ((value & 4294965248) === 0) {
          output[offset++] = value >> 6 & 31 | 192;
        } else {
          if (value >= 55296 && value <= 56319) {
            if (pos < strLength) {
              const extra = str.charCodeAt(pos);
              if ((extra & 64512) === 56320) {
                ++pos;
                value = ((value & 1023) << 10) + (extra & 1023) + 65536;
              }
            }
          }
          if ((value & 4294901760) === 0) {
            output[offset++] = value >> 12 & 15 | 224;
            output[offset++] = value >> 6 & 63 | 128;
          } else {
            output[offset++] = value >> 18 & 7 | 240;
            output[offset++] = value >> 12 & 63 | 128;
            output[offset++] = value >> 6 & 63 | 128;
          }
        }
        output[offset++] = value & 63 | 128;
      }
    }
    var sharedTextEncoder = new TextEncoder();
    var TEXT_ENCODER_THRESHOLD = 50;
    function utf8EncodeTE(str, output, outputOffset) {
      sharedTextEncoder.encodeInto(str, output.subarray(outputOffset));
    }
    function utf8Encode(str, output, outputOffset) {
      if (str.length > TEXT_ENCODER_THRESHOLD) {
        utf8EncodeTE(str, output, outputOffset);
      } else {
        utf8EncodeJs(str, output, outputOffset);
      }
    }
    var CHUNK_SIZE = 4096;
    function utf8DecodeJs(bytes, inputOffset, byteLength) {
      let offset = inputOffset;
      const end = offset + byteLength;
      const units = [];
      let result = "";
      while (offset < end) {
        const byte1 = bytes[offset++];
        if ((byte1 & 128) === 0) {
          units.push(byte1);
        } else if ((byte1 & 224) === 192) {
          const byte2 = bytes[offset++] & 63;
          units.push((byte1 & 31) << 6 | byte2);
        } else if ((byte1 & 240) === 224) {
          const byte2 = bytes[offset++] & 63;
          const byte3 = bytes[offset++] & 63;
          units.push((byte1 & 31) << 12 | byte2 << 6 | byte3);
        } else if ((byte1 & 248) === 240) {
          const byte2 = bytes[offset++] & 63;
          const byte3 = bytes[offset++] & 63;
          const byte4 = bytes[offset++] & 63;
          let unit = (byte1 & 7) << 18 | byte2 << 12 | byte3 << 6 | byte4;
          if (unit > 65535) {
            unit -= 65536;
            units.push(unit >>> 10 & 1023 | 55296);
            unit = 56320 | unit & 1023;
          }
          units.push(unit);
        } else {
          units.push(byte1);
        }
        if (units.length >= CHUNK_SIZE) {
          result += String.fromCharCode(...units);
          units.length = 0;
        }
      }
      if (units.length > 0) {
        result += String.fromCharCode(...units);
      }
      return result;
    }
    var sharedTextDecoder = new TextDecoder();
    var TEXT_DECODER_THRESHOLD = 200;
    function utf8DecodeTD(bytes, inputOffset, byteLength) {
      const stringBytes = bytes.subarray(inputOffset, inputOffset + byteLength);
      return sharedTextDecoder.decode(stringBytes);
    }
    function utf8Decode(bytes, inputOffset, byteLength) {
      if (byteLength > TEXT_DECODER_THRESHOLD) {
        return utf8DecodeTD(bytes, inputOffset, byteLength);
      } else {
        return utf8DecodeJs(bytes, inputOffset, byteLength);
      }
    }
  }
});

// ../node_modules/.pnpm/@msgpack+msgpack@3.1.3/node_modules/@msgpack/msgpack/dist.cjs/ExtData.cjs
var require_ExtData = __commonJS({
  "../node_modules/.pnpm/@msgpack+msgpack@3.1.3/node_modules/@msgpack/msgpack/dist.cjs/ExtData.cjs"(exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.ExtData = void 0;
    var ExtData = class {
      type;
      data;
      constructor(type, data) {
        this.type = type;
        this.data = data;
      }
    };
    exports.ExtData = ExtData;
  }
});

// ../node_modules/.pnpm/@msgpack+msgpack@3.1.3/node_modules/@msgpack/msgpack/dist.cjs/DecodeError.cjs
var require_DecodeError = __commonJS({
  "../node_modules/.pnpm/@msgpack+msgpack@3.1.3/node_modules/@msgpack/msgpack/dist.cjs/DecodeError.cjs"(exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.DecodeError = void 0;
    var DecodeError = class _DecodeError extends Error {
      constructor(message) {
        super(message);
        const proto = Object.create(_DecodeError.prototype);
        Object.setPrototypeOf(this, proto);
        Object.defineProperty(this, "name", {
          configurable: true,
          enumerable: false,
          value: _DecodeError.name
        });
      }
    };
    exports.DecodeError = DecodeError;
  }
});

// ../node_modules/.pnpm/@msgpack+msgpack@3.1.3/node_modules/@msgpack/msgpack/dist.cjs/utils/int.cjs
var require_int = __commonJS({
  "../node_modules/.pnpm/@msgpack+msgpack@3.1.3/node_modules/@msgpack/msgpack/dist.cjs/utils/int.cjs"(exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.UINT32_MAX = void 0;
    exports.setUint64 = setUint64;
    exports.setInt64 = setInt64;
    exports.getInt64 = getInt64;
    exports.getUint64 = getUint64;
    exports.UINT32_MAX = 4294967295;
    function setUint64(view, offset, value) {
      const high = value / 4294967296;
      const low = value;
      view.setUint32(offset, high);
      view.setUint32(offset + 4, low);
    }
    function setInt64(view, offset, value) {
      const high = Math.floor(value / 4294967296);
      const low = value;
      view.setUint32(offset, high);
      view.setUint32(offset + 4, low);
    }
    function getInt64(view, offset) {
      const high = view.getInt32(offset);
      const low = view.getUint32(offset + 4);
      return high * 4294967296 + low;
    }
    function getUint64(view, offset) {
      const high = view.getUint32(offset);
      const low = view.getUint32(offset + 4);
      return high * 4294967296 + low;
    }
  }
});

// ../node_modules/.pnpm/@msgpack+msgpack@3.1.3/node_modules/@msgpack/msgpack/dist.cjs/timestamp.cjs
var require_timestamp = __commonJS({
  "../node_modules/.pnpm/@msgpack+msgpack@3.1.3/node_modules/@msgpack/msgpack/dist.cjs/timestamp.cjs"(exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.timestampExtension = exports.EXT_TIMESTAMP = void 0;
    exports.encodeTimeSpecToTimestamp = encodeTimeSpecToTimestamp;
    exports.encodeDateToTimeSpec = encodeDateToTimeSpec;
    exports.encodeTimestampExtension = encodeTimestampExtension;
    exports.decodeTimestampToTimeSpec = decodeTimestampToTimeSpec;
    exports.decodeTimestampExtension = decodeTimestampExtension;
    var DecodeError_ts_1 = require_DecodeError();
    var int_ts_1 = require_int();
    exports.EXT_TIMESTAMP = -1;
    var TIMESTAMP32_MAX_SEC = 4294967296 - 1;
    var TIMESTAMP64_MAX_SEC = 17179869184 - 1;
    function encodeTimeSpecToTimestamp({ sec, nsec }) {
      if (sec >= 0 && nsec >= 0 && sec <= TIMESTAMP64_MAX_SEC) {
        if (nsec === 0 && sec <= TIMESTAMP32_MAX_SEC) {
          const rv = new Uint8Array(4);
          const view = new DataView(rv.buffer);
          view.setUint32(0, sec);
          return rv;
        } else {
          const secHigh = sec / 4294967296;
          const secLow = sec & 4294967295;
          const rv = new Uint8Array(8);
          const view = new DataView(rv.buffer);
          view.setUint32(0, nsec << 2 | secHigh & 3);
          view.setUint32(4, secLow);
          return rv;
        }
      } else {
        const rv = new Uint8Array(12);
        const view = new DataView(rv.buffer);
        view.setUint32(0, nsec);
        (0, int_ts_1.setInt64)(view, 4, sec);
        return rv;
      }
    }
    function encodeDateToTimeSpec(date) {
      const msec = date.getTime();
      const sec = Math.floor(msec / 1e3);
      const nsec = (msec - sec * 1e3) * 1e6;
      const nsecInSec = Math.floor(nsec / 1e9);
      return {
        sec: sec + nsecInSec,
        nsec: nsec - nsecInSec * 1e9
      };
    }
    function encodeTimestampExtension(object) {
      if (object instanceof Date) {
        const timeSpec = encodeDateToTimeSpec(object);
        return encodeTimeSpecToTimestamp(timeSpec);
      } else {
        return null;
      }
    }
    function decodeTimestampToTimeSpec(data) {
      const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
      switch (data.byteLength) {
        case 4: {
          const sec = view.getUint32(0);
          const nsec = 0;
          return { sec, nsec };
        }
        case 8: {
          const nsec30AndSecHigh2 = view.getUint32(0);
          const secLow32 = view.getUint32(4);
          const sec = (nsec30AndSecHigh2 & 3) * 4294967296 + secLow32;
          const nsec = nsec30AndSecHigh2 >>> 2;
          return { sec, nsec };
        }
        case 12: {
          const sec = (0, int_ts_1.getInt64)(view, 4);
          const nsec = view.getUint32(0);
          return { sec, nsec };
        }
        default:
          throw new DecodeError_ts_1.DecodeError(`Unrecognized data size for timestamp (expected 4, 8, or 12): ${data.length}`);
      }
    }
    function decodeTimestampExtension(data) {
      const timeSpec = decodeTimestampToTimeSpec(data);
      return new Date(timeSpec.sec * 1e3 + timeSpec.nsec / 1e6);
    }
    exports.timestampExtension = {
      type: exports.EXT_TIMESTAMP,
      encode: encodeTimestampExtension,
      decode: decodeTimestampExtension
    };
  }
});

// ../node_modules/.pnpm/@msgpack+msgpack@3.1.3/node_modules/@msgpack/msgpack/dist.cjs/ExtensionCodec.cjs
var require_ExtensionCodec = __commonJS({
  "../node_modules/.pnpm/@msgpack+msgpack@3.1.3/node_modules/@msgpack/msgpack/dist.cjs/ExtensionCodec.cjs"(exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.ExtensionCodec = void 0;
    var ExtData_ts_1 = require_ExtData();
    var timestamp_ts_1 = require_timestamp();
    var ExtensionCodec = class _ExtensionCodec {
      static defaultCodec = new _ExtensionCodec();
      // ensures ExtensionCodecType<X> matches ExtensionCodec<X>
      // this will make type errors a lot more clear
      // eslint-disable-next-line @typescript-eslint/naming-convention
      __brand;
      // built-in extensions
      builtInEncoders = [];
      builtInDecoders = [];
      // custom extensions
      encoders = [];
      decoders = [];
      constructor() {
        this.register(timestamp_ts_1.timestampExtension);
      }
      register({ type, encode, decode }) {
        if (type >= 0) {
          this.encoders[type] = encode;
          this.decoders[type] = decode;
        } else {
          const index = -1 - type;
          this.builtInEncoders[index] = encode;
          this.builtInDecoders[index] = decode;
        }
      }
      tryToEncode(object, context) {
        for (let i = 0; i < this.builtInEncoders.length; i++) {
          const encodeExt = this.builtInEncoders[i];
          if (encodeExt != null) {
            const data = encodeExt(object, context);
            if (data != null) {
              const type = -1 - i;
              return new ExtData_ts_1.ExtData(type, data);
            }
          }
        }
        for (let i = 0; i < this.encoders.length; i++) {
          const encodeExt = this.encoders[i];
          if (encodeExt != null) {
            const data = encodeExt(object, context);
            if (data != null) {
              const type = i;
              return new ExtData_ts_1.ExtData(type, data);
            }
          }
        }
        if (object instanceof ExtData_ts_1.ExtData) {
          return object;
        }
        return null;
      }
      decode(data, type, context) {
        const decodeExt = type < 0 ? this.builtInDecoders[-1 - type] : this.decoders[type];
        if (decodeExt) {
          return decodeExt(data, type, context);
        } else {
          return new ExtData_ts_1.ExtData(type, data);
        }
      }
    };
    exports.ExtensionCodec = ExtensionCodec;
  }
});

// ../node_modules/.pnpm/@msgpack+msgpack@3.1.3/node_modules/@msgpack/msgpack/dist.cjs/utils/typedArrays.cjs
var require_typedArrays = __commonJS({
  "../node_modules/.pnpm/@msgpack+msgpack@3.1.3/node_modules/@msgpack/msgpack/dist.cjs/utils/typedArrays.cjs"(exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.ensureUint8Array = ensureUint8Array;
    function isArrayBufferLike(buffer) {
      return buffer instanceof ArrayBuffer || typeof SharedArrayBuffer !== "undefined" && buffer instanceof SharedArrayBuffer;
    }
    function ensureUint8Array(buffer) {
      if (buffer instanceof Uint8Array) {
        return buffer;
      } else if (ArrayBuffer.isView(buffer)) {
        return new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength);
      } else if (isArrayBufferLike(buffer)) {
        return new Uint8Array(buffer);
      } else {
        return Uint8Array.from(buffer);
      }
    }
  }
});

// ../node_modules/.pnpm/@msgpack+msgpack@3.1.3/node_modules/@msgpack/msgpack/dist.cjs/Encoder.cjs
var require_Encoder = __commonJS({
  "../node_modules/.pnpm/@msgpack+msgpack@3.1.3/node_modules/@msgpack/msgpack/dist.cjs/Encoder.cjs"(exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.Encoder = exports.DEFAULT_INITIAL_BUFFER_SIZE = exports.DEFAULT_MAX_DEPTH = void 0;
    var utf8_ts_1 = require_utf8();
    var ExtensionCodec_ts_1 = require_ExtensionCodec();
    var int_ts_1 = require_int();
    var typedArrays_ts_1 = require_typedArrays();
    exports.DEFAULT_MAX_DEPTH = 100;
    exports.DEFAULT_INITIAL_BUFFER_SIZE = 2048;
    var Encoder = class _Encoder {
      extensionCodec;
      context;
      useBigInt64;
      maxDepth;
      initialBufferSize;
      sortKeys;
      forceFloat32;
      ignoreUndefined;
      forceIntegerToFloat;
      pos;
      view;
      bytes;
      entered = false;
      constructor(options) {
        this.extensionCodec = options?.extensionCodec ?? ExtensionCodec_ts_1.ExtensionCodec.defaultCodec;
        this.context = options?.context;
        this.useBigInt64 = options?.useBigInt64 ?? false;
        this.maxDepth = options?.maxDepth ?? exports.DEFAULT_MAX_DEPTH;
        this.initialBufferSize = options?.initialBufferSize ?? exports.DEFAULT_INITIAL_BUFFER_SIZE;
        this.sortKeys = options?.sortKeys ?? false;
        this.forceFloat32 = options?.forceFloat32 ?? false;
        this.ignoreUndefined = options?.ignoreUndefined ?? false;
        this.forceIntegerToFloat = options?.forceIntegerToFloat ?? false;
        this.pos = 0;
        this.view = new DataView(new ArrayBuffer(this.initialBufferSize));
        this.bytes = new Uint8Array(this.view.buffer);
      }
      clone() {
        return new _Encoder({
          extensionCodec: this.extensionCodec,
          context: this.context,
          useBigInt64: this.useBigInt64,
          maxDepth: this.maxDepth,
          initialBufferSize: this.initialBufferSize,
          sortKeys: this.sortKeys,
          forceFloat32: this.forceFloat32,
          ignoreUndefined: this.ignoreUndefined,
          forceIntegerToFloat: this.forceIntegerToFloat
        });
      }
      reinitializeState() {
        this.pos = 0;
      }
      /**
       * This is almost equivalent to {@link Encoder#encode}, but it returns an reference of the encoder's internal buffer and thus much faster than {@link Encoder#encode}.
       *
       * @returns Encodes the object and returns a shared reference the encoder's internal buffer.
       */
      encodeSharedRef(object) {
        if (this.entered) {
          const instance = this.clone();
          return instance.encodeSharedRef(object);
        }
        try {
          this.entered = true;
          this.reinitializeState();
          this.doEncode(object, 1);
          return this.bytes.subarray(0, this.pos);
        } finally {
          this.entered = false;
        }
      }
      /**
       * @returns Encodes the object and returns a copy of the encoder's internal buffer.
       */
      encode(object) {
        if (this.entered) {
          const instance = this.clone();
          return instance.encode(object);
        }
        try {
          this.entered = true;
          this.reinitializeState();
          this.doEncode(object, 1);
          return this.bytes.slice(0, this.pos);
        } finally {
          this.entered = false;
        }
      }
      doEncode(object, depth) {
        if (depth > this.maxDepth) {
          throw new Error(`Too deep objects in depth ${depth}`);
        }
        if (object == null) {
          this.encodeNil();
        } else if (typeof object === "boolean") {
          this.encodeBoolean(object);
        } else if (typeof object === "number") {
          if (!this.forceIntegerToFloat) {
            this.encodeNumber(object);
          } else {
            this.encodeNumberAsFloat(object);
          }
        } else if (typeof object === "string") {
          this.encodeString(object);
        } else if (this.useBigInt64 && typeof object === "bigint") {
          this.encodeBigInt64(object);
        } else {
          this.encodeObject(object, depth);
        }
      }
      ensureBufferSizeToWrite(sizeToWrite) {
        const requiredSize = this.pos + sizeToWrite;
        if (this.view.byteLength < requiredSize) {
          this.resizeBuffer(requiredSize * 2);
        }
      }
      resizeBuffer(newSize) {
        const newBuffer = new ArrayBuffer(newSize);
        const newBytes = new Uint8Array(newBuffer);
        const newView = new DataView(newBuffer);
        newBytes.set(this.bytes);
        this.view = newView;
        this.bytes = newBytes;
      }
      encodeNil() {
        this.writeU8(192);
      }
      encodeBoolean(object) {
        if (object === false) {
          this.writeU8(194);
        } else {
          this.writeU8(195);
        }
      }
      encodeNumber(object) {
        if (!this.forceIntegerToFloat && Number.isSafeInteger(object)) {
          if (object >= 0) {
            if (object < 128) {
              this.writeU8(object);
            } else if (object < 256) {
              this.writeU8(204);
              this.writeU8(object);
            } else if (object < 65536) {
              this.writeU8(205);
              this.writeU16(object);
            } else if (object < 4294967296) {
              this.writeU8(206);
              this.writeU32(object);
            } else if (!this.useBigInt64) {
              this.writeU8(207);
              this.writeU64(object);
            } else {
              this.encodeNumberAsFloat(object);
            }
          } else {
            if (object >= -32) {
              this.writeU8(224 | object + 32);
            } else if (object >= -128) {
              this.writeU8(208);
              this.writeI8(object);
            } else if (object >= -32768) {
              this.writeU8(209);
              this.writeI16(object);
            } else if (object >= -2147483648) {
              this.writeU8(210);
              this.writeI32(object);
            } else if (!this.useBigInt64) {
              this.writeU8(211);
              this.writeI64(object);
            } else {
              this.encodeNumberAsFloat(object);
            }
          }
        } else {
          this.encodeNumberAsFloat(object);
        }
      }
      encodeNumberAsFloat(object) {
        if (this.forceFloat32) {
          this.writeU8(202);
          this.writeF32(object);
        } else {
          this.writeU8(203);
          this.writeF64(object);
        }
      }
      encodeBigInt64(object) {
        if (object >= BigInt(0)) {
          this.writeU8(207);
          this.writeBigUint64(object);
        } else {
          this.writeU8(211);
          this.writeBigInt64(object);
        }
      }
      writeStringHeader(byteLength) {
        if (byteLength < 32) {
          this.writeU8(160 + byteLength);
        } else if (byteLength < 256) {
          this.writeU8(217);
          this.writeU8(byteLength);
        } else if (byteLength < 65536) {
          this.writeU8(218);
          this.writeU16(byteLength);
        } else if (byteLength < 4294967296) {
          this.writeU8(219);
          this.writeU32(byteLength);
        } else {
          throw new Error(`Too long string: ${byteLength} bytes in UTF-8`);
        }
      }
      encodeString(object) {
        const maxHeaderSize = 1 + 4;
        const byteLength = (0, utf8_ts_1.utf8Count)(object);
        this.ensureBufferSizeToWrite(maxHeaderSize + byteLength);
        this.writeStringHeader(byteLength);
        (0, utf8_ts_1.utf8Encode)(object, this.bytes, this.pos);
        this.pos += byteLength;
      }
      encodeObject(object, depth) {
        const ext = this.extensionCodec.tryToEncode(object, this.context);
        if (ext != null) {
          this.encodeExtension(ext);
        } else if (Array.isArray(object)) {
          this.encodeArray(object, depth);
        } else if (ArrayBuffer.isView(object)) {
          this.encodeBinary(object);
        } else if (typeof object === "object") {
          this.encodeMap(object, depth);
        } else {
          throw new Error(`Unrecognized object: ${Object.prototype.toString.apply(object)}`);
        }
      }
      encodeBinary(object) {
        const size = object.byteLength;
        if (size < 256) {
          this.writeU8(196);
          this.writeU8(size);
        } else if (size < 65536) {
          this.writeU8(197);
          this.writeU16(size);
        } else if (size < 4294967296) {
          this.writeU8(198);
          this.writeU32(size);
        } else {
          throw new Error(`Too large binary: ${size}`);
        }
        const bytes = (0, typedArrays_ts_1.ensureUint8Array)(object);
        this.writeU8a(bytes);
      }
      encodeArray(object, depth) {
        const size = object.length;
        if (size < 16) {
          this.writeU8(144 + size);
        } else if (size < 65536) {
          this.writeU8(220);
          this.writeU16(size);
        } else if (size < 4294967296) {
          this.writeU8(221);
          this.writeU32(size);
        } else {
          throw new Error(`Too large array: ${size}`);
        }
        for (const item of object) {
          this.doEncode(item, depth + 1);
        }
      }
      countWithoutUndefined(object, keys) {
        let count = 0;
        for (const key of keys) {
          if (object[key] !== void 0) {
            count++;
          }
        }
        return count;
      }
      encodeMap(object, depth) {
        const keys = Object.keys(object);
        if (this.sortKeys) {
          keys.sort();
        }
        const size = this.ignoreUndefined ? this.countWithoutUndefined(object, keys) : keys.length;
        if (size < 16) {
          this.writeU8(128 + size);
        } else if (size < 65536) {
          this.writeU8(222);
          this.writeU16(size);
        } else if (size < 4294967296) {
          this.writeU8(223);
          this.writeU32(size);
        } else {
          throw new Error(`Too large map object: ${size}`);
        }
        for (const key of keys) {
          const value = object[key];
          if (!(this.ignoreUndefined && value === void 0)) {
            this.encodeString(key);
            this.doEncode(value, depth + 1);
          }
        }
      }
      encodeExtension(ext) {
        if (typeof ext.data === "function") {
          const data = ext.data(this.pos + 6);
          const size2 = data.length;
          if (size2 >= 4294967296) {
            throw new Error(`Too large extension object: ${size2}`);
          }
          this.writeU8(201);
          this.writeU32(size2);
          this.writeI8(ext.type);
          this.writeU8a(data);
          return;
        }
        const size = ext.data.length;
        if (size === 1) {
          this.writeU8(212);
        } else if (size === 2) {
          this.writeU8(213);
        } else if (size === 4) {
          this.writeU8(214);
        } else if (size === 8) {
          this.writeU8(215);
        } else if (size === 16) {
          this.writeU8(216);
        } else if (size < 256) {
          this.writeU8(199);
          this.writeU8(size);
        } else if (size < 65536) {
          this.writeU8(200);
          this.writeU16(size);
        } else if (size < 4294967296) {
          this.writeU8(201);
          this.writeU32(size);
        } else {
          throw new Error(`Too large extension object: ${size}`);
        }
        this.writeI8(ext.type);
        this.writeU8a(ext.data);
      }
      writeU8(value) {
        this.ensureBufferSizeToWrite(1);
        this.view.setUint8(this.pos, value);
        this.pos++;
      }
      writeU8a(values) {
        const size = values.length;
        this.ensureBufferSizeToWrite(size);
        this.bytes.set(values, this.pos);
        this.pos += size;
      }
      writeI8(value) {
        this.ensureBufferSizeToWrite(1);
        this.view.setInt8(this.pos, value);
        this.pos++;
      }
      writeU16(value) {
        this.ensureBufferSizeToWrite(2);
        this.view.setUint16(this.pos, value);
        this.pos += 2;
      }
      writeI16(value) {
        this.ensureBufferSizeToWrite(2);
        this.view.setInt16(this.pos, value);
        this.pos += 2;
      }
      writeU32(value) {
        this.ensureBufferSizeToWrite(4);
        this.view.setUint32(this.pos, value);
        this.pos += 4;
      }
      writeI32(value) {
        this.ensureBufferSizeToWrite(4);
        this.view.setInt32(this.pos, value);
        this.pos += 4;
      }
      writeF32(value) {
        this.ensureBufferSizeToWrite(4);
        this.view.setFloat32(this.pos, value);
        this.pos += 4;
      }
      writeF64(value) {
        this.ensureBufferSizeToWrite(8);
        this.view.setFloat64(this.pos, value);
        this.pos += 8;
      }
      writeU64(value) {
        this.ensureBufferSizeToWrite(8);
        (0, int_ts_1.setUint64)(this.view, this.pos, value);
        this.pos += 8;
      }
      writeI64(value) {
        this.ensureBufferSizeToWrite(8);
        (0, int_ts_1.setInt64)(this.view, this.pos, value);
        this.pos += 8;
      }
      writeBigUint64(value) {
        this.ensureBufferSizeToWrite(8);
        this.view.setBigUint64(this.pos, value);
        this.pos += 8;
      }
      writeBigInt64(value) {
        this.ensureBufferSizeToWrite(8);
        this.view.setBigInt64(this.pos, value);
        this.pos += 8;
      }
    };
    exports.Encoder = Encoder;
  }
});

// ../node_modules/.pnpm/@msgpack+msgpack@3.1.3/node_modules/@msgpack/msgpack/dist.cjs/encode.cjs
var require_encode = __commonJS({
  "../node_modules/.pnpm/@msgpack+msgpack@3.1.3/node_modules/@msgpack/msgpack/dist.cjs/encode.cjs"(exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.encode = encode;
    var Encoder_ts_1 = require_Encoder();
    function encode(value, options) {
      const encoder = new Encoder_ts_1.Encoder(options);
      return encoder.encodeSharedRef(value);
    }
  }
});

// ../node_modules/.pnpm/@msgpack+msgpack@3.1.3/node_modules/@msgpack/msgpack/dist.cjs/utils/prettyByte.cjs
var require_prettyByte = __commonJS({
  "../node_modules/.pnpm/@msgpack+msgpack@3.1.3/node_modules/@msgpack/msgpack/dist.cjs/utils/prettyByte.cjs"(exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.prettyByte = prettyByte;
    function prettyByte(byte) {
      return `${byte < 0 ? "-" : ""}0x${Math.abs(byte).toString(16).padStart(2, "0")}`;
    }
  }
});

// ../node_modules/.pnpm/@msgpack+msgpack@3.1.3/node_modules/@msgpack/msgpack/dist.cjs/CachedKeyDecoder.cjs
var require_CachedKeyDecoder = __commonJS({
  "../node_modules/.pnpm/@msgpack+msgpack@3.1.3/node_modules/@msgpack/msgpack/dist.cjs/CachedKeyDecoder.cjs"(exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.CachedKeyDecoder = void 0;
    var utf8_ts_1 = require_utf8();
    var DEFAULT_MAX_KEY_LENGTH = 16;
    var DEFAULT_MAX_LENGTH_PER_KEY = 16;
    var CachedKeyDecoder = class {
      hit = 0;
      miss = 0;
      caches;
      maxKeyLength;
      maxLengthPerKey;
      constructor(maxKeyLength = DEFAULT_MAX_KEY_LENGTH, maxLengthPerKey = DEFAULT_MAX_LENGTH_PER_KEY) {
        this.maxKeyLength = maxKeyLength;
        this.maxLengthPerKey = maxLengthPerKey;
        this.caches = [];
        for (let i = 0; i < this.maxKeyLength; i++) {
          this.caches.push([]);
        }
      }
      canBeCached(byteLength) {
        return byteLength > 0 && byteLength <= this.maxKeyLength;
      }
      find(bytes, inputOffset, byteLength) {
        const records = this.caches[byteLength - 1];
        FIND_CHUNK: for (const record of records) {
          const recordBytes = record.bytes;
          for (let j = 0; j < byteLength; j++) {
            if (recordBytes[j] !== bytes[inputOffset + j]) {
              continue FIND_CHUNK;
            }
          }
          return record.str;
        }
        return null;
      }
      store(bytes, value) {
        const records = this.caches[bytes.length - 1];
        const record = { bytes, str: value };
        if (records.length >= this.maxLengthPerKey) {
          records[Math.random() * records.length | 0] = record;
        } else {
          records.push(record);
        }
      }
      decode(bytes, inputOffset, byteLength) {
        const cachedValue = this.find(bytes, inputOffset, byteLength);
        if (cachedValue != null) {
          this.hit++;
          return cachedValue;
        }
        this.miss++;
        const str = (0, utf8_ts_1.utf8DecodeJs)(bytes, inputOffset, byteLength);
        const slicedCopyOfBytes = Uint8Array.prototype.slice.call(bytes, inputOffset, inputOffset + byteLength);
        this.store(slicedCopyOfBytes, str);
        return str;
      }
    };
    exports.CachedKeyDecoder = CachedKeyDecoder;
  }
});

// ../node_modules/.pnpm/@msgpack+msgpack@3.1.3/node_modules/@msgpack/msgpack/dist.cjs/Decoder.cjs
var require_Decoder = __commonJS({
  "../node_modules/.pnpm/@msgpack+msgpack@3.1.3/node_modules/@msgpack/msgpack/dist.cjs/Decoder.cjs"(exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.Decoder = void 0;
    var prettyByte_ts_1 = require_prettyByte();
    var ExtensionCodec_ts_1 = require_ExtensionCodec();
    var int_ts_1 = require_int();
    var utf8_ts_1 = require_utf8();
    var typedArrays_ts_1 = require_typedArrays();
    var CachedKeyDecoder_ts_1 = require_CachedKeyDecoder();
    var DecodeError_ts_1 = require_DecodeError();
    var STATE_ARRAY = "array";
    var STATE_MAP_KEY = "map_key";
    var STATE_MAP_VALUE = "map_value";
    var mapKeyConverter = (key) => {
      if (typeof key === "string" || typeof key === "number") {
        return key;
      }
      throw new DecodeError_ts_1.DecodeError("The type of key must be string or number but " + typeof key);
    };
    var StackPool = class {
      stack = [];
      stackHeadPosition = -1;
      get length() {
        return this.stackHeadPosition + 1;
      }
      top() {
        return this.stack[this.stackHeadPosition];
      }
      pushArrayState(size) {
        const state = this.getUninitializedStateFromPool();
        state.type = STATE_ARRAY;
        state.position = 0;
        state.size = size;
        state.array = new Array(size);
      }
      pushMapState(size) {
        const state = this.getUninitializedStateFromPool();
        state.type = STATE_MAP_KEY;
        state.readCount = 0;
        state.size = size;
        state.map = {};
      }
      getUninitializedStateFromPool() {
        this.stackHeadPosition++;
        if (this.stackHeadPosition === this.stack.length) {
          const partialState = {
            type: void 0,
            size: 0,
            array: void 0,
            position: 0,
            readCount: 0,
            map: void 0,
            key: null
          };
          this.stack.push(partialState);
        }
        return this.stack[this.stackHeadPosition];
      }
      release(state) {
        const topStackState = this.stack[this.stackHeadPosition];
        if (topStackState !== state) {
          throw new Error("Invalid stack state. Released state is not on top of the stack.");
        }
        if (state.type === STATE_ARRAY) {
          const partialState = state;
          partialState.size = 0;
          partialState.array = void 0;
          partialState.position = 0;
          partialState.type = void 0;
        }
        if (state.type === STATE_MAP_KEY || state.type === STATE_MAP_VALUE) {
          const partialState = state;
          partialState.size = 0;
          partialState.map = void 0;
          partialState.readCount = 0;
          partialState.type = void 0;
        }
        this.stackHeadPosition--;
      }
      reset() {
        this.stack.length = 0;
        this.stackHeadPosition = -1;
      }
    };
    var HEAD_BYTE_REQUIRED = -1;
    var EMPTY_VIEW = new DataView(new ArrayBuffer(0));
    var EMPTY_BYTES = new Uint8Array(EMPTY_VIEW.buffer);
    try {
      EMPTY_VIEW.getInt8(0);
    } catch (e) {
      if (!(e instanceof RangeError)) {
        throw new Error("This module is not supported in the current JavaScript engine because DataView does not throw RangeError on out-of-bounds access");
      }
    }
    var MORE_DATA = new RangeError("Insufficient data");
    var sharedCachedKeyDecoder = new CachedKeyDecoder_ts_1.CachedKeyDecoder();
    var Decoder = class _Decoder {
      extensionCodec;
      context;
      useBigInt64;
      rawStrings;
      maxStrLength;
      maxBinLength;
      maxArrayLength;
      maxMapLength;
      maxExtLength;
      keyDecoder;
      mapKeyConverter;
      totalPos = 0;
      pos = 0;
      view = EMPTY_VIEW;
      bytes = EMPTY_BYTES;
      headByte = HEAD_BYTE_REQUIRED;
      stack = new StackPool();
      entered = false;
      constructor(options) {
        this.extensionCodec = options?.extensionCodec ?? ExtensionCodec_ts_1.ExtensionCodec.defaultCodec;
        this.context = options?.context;
        this.useBigInt64 = options?.useBigInt64 ?? false;
        this.rawStrings = options?.rawStrings ?? false;
        this.maxStrLength = options?.maxStrLength ?? int_ts_1.UINT32_MAX;
        this.maxBinLength = options?.maxBinLength ?? int_ts_1.UINT32_MAX;
        this.maxArrayLength = options?.maxArrayLength ?? int_ts_1.UINT32_MAX;
        this.maxMapLength = options?.maxMapLength ?? int_ts_1.UINT32_MAX;
        this.maxExtLength = options?.maxExtLength ?? int_ts_1.UINT32_MAX;
        this.keyDecoder = options?.keyDecoder !== void 0 ? options.keyDecoder : sharedCachedKeyDecoder;
        this.mapKeyConverter = options?.mapKeyConverter ?? mapKeyConverter;
      }
      clone() {
        return new _Decoder({
          extensionCodec: this.extensionCodec,
          context: this.context,
          useBigInt64: this.useBigInt64,
          rawStrings: this.rawStrings,
          maxStrLength: this.maxStrLength,
          maxBinLength: this.maxBinLength,
          maxArrayLength: this.maxArrayLength,
          maxMapLength: this.maxMapLength,
          maxExtLength: this.maxExtLength,
          keyDecoder: this.keyDecoder
        });
      }
      reinitializeState() {
        this.totalPos = 0;
        this.headByte = HEAD_BYTE_REQUIRED;
        this.stack.reset();
      }
      setBuffer(buffer) {
        const bytes = (0, typedArrays_ts_1.ensureUint8Array)(buffer);
        this.bytes = bytes;
        this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
        this.pos = 0;
      }
      appendBuffer(buffer) {
        if (this.headByte === HEAD_BYTE_REQUIRED && !this.hasRemaining(1)) {
          this.setBuffer(buffer);
        } else {
          const remainingData = this.bytes.subarray(this.pos);
          const newData = (0, typedArrays_ts_1.ensureUint8Array)(buffer);
          const newBuffer = new Uint8Array(remainingData.length + newData.length);
          newBuffer.set(remainingData);
          newBuffer.set(newData, remainingData.length);
          this.setBuffer(newBuffer);
        }
      }
      hasRemaining(size) {
        return this.view.byteLength - this.pos >= size;
      }
      createExtraByteError(posToShow) {
        const { view, pos } = this;
        return new RangeError(`Extra ${view.byteLength - pos} of ${view.byteLength} byte(s) found at buffer[${posToShow}]`);
      }
      /**
       * @throws {@link DecodeError}
       * @throws {@link RangeError}
       */
      decode(buffer) {
        if (this.entered) {
          const instance = this.clone();
          return instance.decode(buffer);
        }
        try {
          this.entered = true;
          this.reinitializeState();
          this.setBuffer(buffer);
          const object = this.doDecodeSync();
          if (this.hasRemaining(1)) {
            throw this.createExtraByteError(this.pos);
          }
          return object;
        } finally {
          this.entered = false;
        }
      }
      *decodeMulti(buffer) {
        if (this.entered) {
          const instance = this.clone();
          yield* instance.decodeMulti(buffer);
          return;
        }
        try {
          this.entered = true;
          this.reinitializeState();
          this.setBuffer(buffer);
          while (this.hasRemaining(1)) {
            yield this.doDecodeSync();
          }
        } finally {
          this.entered = false;
        }
      }
      async decodeAsync(stream) {
        if (this.entered) {
          const instance = this.clone();
          return instance.decodeAsync(stream);
        }
        try {
          this.entered = true;
          let decoded = false;
          let object;
          for await (const buffer of stream) {
            if (decoded) {
              this.entered = false;
              throw this.createExtraByteError(this.totalPos);
            }
            this.appendBuffer(buffer);
            try {
              object = this.doDecodeSync();
              decoded = true;
            } catch (e) {
              if (!(e instanceof RangeError)) {
                throw e;
              }
            }
            this.totalPos += this.pos;
          }
          if (decoded) {
            if (this.hasRemaining(1)) {
              throw this.createExtraByteError(this.totalPos);
            }
            return object;
          }
          const { headByte, pos, totalPos } = this;
          throw new RangeError(`Insufficient data in parsing ${(0, prettyByte_ts_1.prettyByte)(headByte)} at ${totalPos} (${pos} in the current buffer)`);
        } finally {
          this.entered = false;
        }
      }
      decodeArrayStream(stream) {
        return this.decodeMultiAsync(stream, true);
      }
      decodeStream(stream) {
        return this.decodeMultiAsync(stream, false);
      }
      async *decodeMultiAsync(stream, isArray) {
        if (this.entered) {
          const instance = this.clone();
          yield* instance.decodeMultiAsync(stream, isArray);
          return;
        }
        try {
          this.entered = true;
          let isArrayHeaderRequired = isArray;
          let arrayItemsLeft = -1;
          for await (const buffer of stream) {
            if (isArray && arrayItemsLeft === 0) {
              throw this.createExtraByteError(this.totalPos);
            }
            this.appendBuffer(buffer);
            if (isArrayHeaderRequired) {
              arrayItemsLeft = this.readArraySize();
              isArrayHeaderRequired = false;
              this.complete();
            }
            try {
              while (true) {
                yield this.doDecodeSync();
                if (--arrayItemsLeft === 0) {
                  break;
                }
              }
            } catch (e) {
              if (!(e instanceof RangeError)) {
                throw e;
              }
            }
            this.totalPos += this.pos;
          }
        } finally {
          this.entered = false;
        }
      }
      doDecodeSync() {
        DECODE: while (true) {
          const headByte = this.readHeadByte();
          let object;
          if (headByte >= 224) {
            object = headByte - 256;
          } else if (headByte < 192) {
            if (headByte < 128) {
              object = headByte;
            } else if (headByte < 144) {
              const size = headByte - 128;
              if (size !== 0) {
                this.pushMapState(size);
                this.complete();
                continue DECODE;
              } else {
                object = {};
              }
            } else if (headByte < 160) {
              const size = headByte - 144;
              if (size !== 0) {
                this.pushArrayState(size);
                this.complete();
                continue DECODE;
              } else {
                object = [];
              }
            } else {
              const byteLength = headByte - 160;
              object = this.decodeString(byteLength, 0);
            }
          } else if (headByte === 192) {
            object = null;
          } else if (headByte === 194) {
            object = false;
          } else if (headByte === 195) {
            object = true;
          } else if (headByte === 202) {
            object = this.readF32();
          } else if (headByte === 203) {
            object = this.readF64();
          } else if (headByte === 204) {
            object = this.readU8();
          } else if (headByte === 205) {
            object = this.readU16();
          } else if (headByte === 206) {
            object = this.readU32();
          } else if (headByte === 207) {
            if (this.useBigInt64) {
              object = this.readU64AsBigInt();
            } else {
              object = this.readU64();
            }
          } else if (headByte === 208) {
            object = this.readI8();
          } else if (headByte === 209) {
            object = this.readI16();
          } else if (headByte === 210) {
            object = this.readI32();
          } else if (headByte === 211) {
            if (this.useBigInt64) {
              object = this.readI64AsBigInt();
            } else {
              object = this.readI64();
            }
          } else if (headByte === 217) {
            const byteLength = this.lookU8();
            object = this.decodeString(byteLength, 1);
          } else if (headByte === 218) {
            const byteLength = this.lookU16();
            object = this.decodeString(byteLength, 2);
          } else if (headByte === 219) {
            const byteLength = this.lookU32();
            object = this.decodeString(byteLength, 4);
          } else if (headByte === 220) {
            const size = this.readU16();
            if (size !== 0) {
              this.pushArrayState(size);
              this.complete();
              continue DECODE;
            } else {
              object = [];
            }
          } else if (headByte === 221) {
            const size = this.readU32();
            if (size !== 0) {
              this.pushArrayState(size);
              this.complete();
              continue DECODE;
            } else {
              object = [];
            }
          } else if (headByte === 222) {
            const size = this.readU16();
            if (size !== 0) {
              this.pushMapState(size);
              this.complete();
              continue DECODE;
            } else {
              object = {};
            }
          } else if (headByte === 223) {
            const size = this.readU32();
            if (size !== 0) {
              this.pushMapState(size);
              this.complete();
              continue DECODE;
            } else {
              object = {};
            }
          } else if (headByte === 196) {
            const size = this.lookU8();
            object = this.decodeBinary(size, 1);
          } else if (headByte === 197) {
            const size = this.lookU16();
            object = this.decodeBinary(size, 2);
          } else if (headByte === 198) {
            const size = this.lookU32();
            object = this.decodeBinary(size, 4);
          } else if (headByte === 212) {
            object = this.decodeExtension(1, 0);
          } else if (headByte === 213) {
            object = this.decodeExtension(2, 0);
          } else if (headByte === 214) {
            object = this.decodeExtension(4, 0);
          } else if (headByte === 215) {
            object = this.decodeExtension(8, 0);
          } else if (headByte === 216) {
            object = this.decodeExtension(16, 0);
          } else if (headByte === 199) {
            const size = this.lookU8();
            object = this.decodeExtension(size, 1);
          } else if (headByte === 200) {
            const size = this.lookU16();
            object = this.decodeExtension(size, 2);
          } else if (headByte === 201) {
            const size = this.lookU32();
            object = this.decodeExtension(size, 4);
          } else {
            throw new DecodeError_ts_1.DecodeError(`Unrecognized type byte: ${(0, prettyByte_ts_1.prettyByte)(headByte)}`);
          }
          this.complete();
          const stack = this.stack;
          while (stack.length > 0) {
            const state = stack.top();
            if (state.type === STATE_ARRAY) {
              state.array[state.position] = object;
              state.position++;
              if (state.position === state.size) {
                object = state.array;
                stack.release(state);
              } else {
                continue DECODE;
              }
            } else if (state.type === STATE_MAP_KEY) {
              if (object === "__proto__") {
                throw new DecodeError_ts_1.DecodeError("The key __proto__ is not allowed");
              }
              state.key = this.mapKeyConverter(object);
              state.type = STATE_MAP_VALUE;
              continue DECODE;
            } else {
              state.map[state.key] = object;
              state.readCount++;
              if (state.readCount === state.size) {
                object = state.map;
                stack.release(state);
              } else {
                state.key = null;
                state.type = STATE_MAP_KEY;
                continue DECODE;
              }
            }
          }
          return object;
        }
      }
      readHeadByte() {
        if (this.headByte === HEAD_BYTE_REQUIRED) {
          this.headByte = this.readU8();
        }
        return this.headByte;
      }
      complete() {
        this.headByte = HEAD_BYTE_REQUIRED;
      }
      readArraySize() {
        const headByte = this.readHeadByte();
        switch (headByte) {
          case 220:
            return this.readU16();
          case 221:
            return this.readU32();
          default: {
            if (headByte < 160) {
              return headByte - 144;
            } else {
              throw new DecodeError_ts_1.DecodeError(`Unrecognized array type byte: ${(0, prettyByte_ts_1.prettyByte)(headByte)}`);
            }
          }
        }
      }
      pushMapState(size) {
        if (size > this.maxMapLength) {
          throw new DecodeError_ts_1.DecodeError(`Max length exceeded: map length (${size}) > maxMapLengthLength (${this.maxMapLength})`);
        }
        this.stack.pushMapState(size);
      }
      pushArrayState(size) {
        if (size > this.maxArrayLength) {
          throw new DecodeError_ts_1.DecodeError(`Max length exceeded: array length (${size}) > maxArrayLength (${this.maxArrayLength})`);
        }
        this.stack.pushArrayState(size);
      }
      decodeString(byteLength, headerOffset) {
        if (!this.rawStrings || this.stateIsMapKey()) {
          return this.decodeUtf8String(byteLength, headerOffset);
        }
        return this.decodeBinary(byteLength, headerOffset);
      }
      /**
       * @throws {@link RangeError}
       */
      decodeUtf8String(byteLength, headerOffset) {
        if (byteLength > this.maxStrLength) {
          throw new DecodeError_ts_1.DecodeError(`Max length exceeded: UTF-8 byte length (${byteLength}) > maxStrLength (${this.maxStrLength})`);
        }
        if (this.bytes.byteLength < this.pos + headerOffset + byteLength) {
          throw MORE_DATA;
        }
        const offset = this.pos + headerOffset;
        let object;
        if (this.stateIsMapKey() && this.keyDecoder?.canBeCached(byteLength)) {
          object = this.keyDecoder.decode(this.bytes, offset, byteLength);
        } else {
          object = (0, utf8_ts_1.utf8Decode)(this.bytes, offset, byteLength);
        }
        this.pos += headerOffset + byteLength;
        return object;
      }
      stateIsMapKey() {
        if (this.stack.length > 0) {
          const state = this.stack.top();
          return state.type === STATE_MAP_KEY;
        }
        return false;
      }
      /**
       * @throws {@link RangeError}
       */
      decodeBinary(byteLength, headOffset) {
        if (byteLength > this.maxBinLength) {
          throw new DecodeError_ts_1.DecodeError(`Max length exceeded: bin length (${byteLength}) > maxBinLength (${this.maxBinLength})`);
        }
        if (!this.hasRemaining(byteLength + headOffset)) {
          throw MORE_DATA;
        }
        const offset = this.pos + headOffset;
        const object = this.bytes.subarray(offset, offset + byteLength);
        this.pos += headOffset + byteLength;
        return object;
      }
      decodeExtension(size, headOffset) {
        if (size > this.maxExtLength) {
          throw new DecodeError_ts_1.DecodeError(`Max length exceeded: ext length (${size}) > maxExtLength (${this.maxExtLength})`);
        }
        const extType = this.view.getInt8(this.pos + headOffset);
        const data = this.decodeBinary(
          size,
          headOffset + 1
          /* extType */
        );
        return this.extensionCodec.decode(data, extType, this.context);
      }
      lookU8() {
        return this.view.getUint8(this.pos);
      }
      lookU16() {
        return this.view.getUint16(this.pos);
      }
      lookU32() {
        return this.view.getUint32(this.pos);
      }
      readU8() {
        const value = this.view.getUint8(this.pos);
        this.pos++;
        return value;
      }
      readI8() {
        const value = this.view.getInt8(this.pos);
        this.pos++;
        return value;
      }
      readU16() {
        const value = this.view.getUint16(this.pos);
        this.pos += 2;
        return value;
      }
      readI16() {
        const value = this.view.getInt16(this.pos);
        this.pos += 2;
        return value;
      }
      readU32() {
        const value = this.view.getUint32(this.pos);
        this.pos += 4;
        return value;
      }
      readI32() {
        const value = this.view.getInt32(this.pos);
        this.pos += 4;
        return value;
      }
      readU64() {
        const value = (0, int_ts_1.getUint64)(this.view, this.pos);
        this.pos += 8;
        return value;
      }
      readI64() {
        const value = (0, int_ts_1.getInt64)(this.view, this.pos);
        this.pos += 8;
        return value;
      }
      readU64AsBigInt() {
        const value = this.view.getBigUint64(this.pos);
        this.pos += 8;
        return value;
      }
      readI64AsBigInt() {
        const value = this.view.getBigInt64(this.pos);
        this.pos += 8;
        return value;
      }
      readF32() {
        const value = this.view.getFloat32(this.pos);
        this.pos += 4;
        return value;
      }
      readF64() {
        const value = this.view.getFloat64(this.pos);
        this.pos += 8;
        return value;
      }
    };
    exports.Decoder = Decoder;
  }
});

// ../node_modules/.pnpm/@msgpack+msgpack@3.1.3/node_modules/@msgpack/msgpack/dist.cjs/decode.cjs
var require_decode = __commonJS({
  "../node_modules/.pnpm/@msgpack+msgpack@3.1.3/node_modules/@msgpack/msgpack/dist.cjs/decode.cjs"(exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.decode = decode;
    exports.decodeMulti = decodeMulti;
    var Decoder_ts_1 = require_Decoder();
    function decode(buffer, options) {
      const decoder = new Decoder_ts_1.Decoder(options);
      return decoder.decode(buffer);
    }
    function decodeMulti(buffer, options) {
      const decoder = new Decoder_ts_1.Decoder(options);
      return decoder.decodeMulti(buffer);
    }
  }
});

// ../node_modules/.pnpm/@msgpack+msgpack@3.1.3/node_modules/@msgpack/msgpack/dist.cjs/utils/stream.cjs
var require_stream = __commonJS({
  "../node_modules/.pnpm/@msgpack+msgpack@3.1.3/node_modules/@msgpack/msgpack/dist.cjs/utils/stream.cjs"(exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.isAsyncIterable = isAsyncIterable;
    exports.asyncIterableFromStream = asyncIterableFromStream;
    exports.ensureAsyncIterable = ensureAsyncIterable;
    function isAsyncIterable(object) {
      return object[Symbol.asyncIterator] != null;
    }
    async function* asyncIterableFromStream(stream) {
      const reader = stream.getReader();
      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) {
            return;
          }
          yield value;
        }
      } finally {
        reader.releaseLock();
      }
    }
    function ensureAsyncIterable(streamLike) {
      if (isAsyncIterable(streamLike)) {
        return streamLike;
      } else {
        return asyncIterableFromStream(streamLike);
      }
    }
  }
});

// ../node_modules/.pnpm/@msgpack+msgpack@3.1.3/node_modules/@msgpack/msgpack/dist.cjs/decodeAsync.cjs
var require_decodeAsync = __commonJS({
  "../node_modules/.pnpm/@msgpack+msgpack@3.1.3/node_modules/@msgpack/msgpack/dist.cjs/decodeAsync.cjs"(exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.decodeAsync = decodeAsync;
    exports.decodeArrayStream = decodeArrayStream;
    exports.decodeMultiStream = decodeMultiStream;
    var Decoder_ts_1 = require_Decoder();
    var stream_ts_1 = require_stream();
    async function decodeAsync(streamLike, options) {
      const stream = (0, stream_ts_1.ensureAsyncIterable)(streamLike);
      const decoder = new Decoder_ts_1.Decoder(options);
      return decoder.decodeAsync(stream);
    }
    function decodeArrayStream(streamLike, options) {
      const stream = (0, stream_ts_1.ensureAsyncIterable)(streamLike);
      const decoder = new Decoder_ts_1.Decoder(options);
      return decoder.decodeArrayStream(stream);
    }
    function decodeMultiStream(streamLike, options) {
      const stream = (0, stream_ts_1.ensureAsyncIterable)(streamLike);
      const decoder = new Decoder_ts_1.Decoder(options);
      return decoder.decodeStream(stream);
    }
  }
});

// ../node_modules/.pnpm/@msgpack+msgpack@3.1.3/node_modules/@msgpack/msgpack/dist.cjs/index.cjs
var require_dist = __commonJS({
  "../node_modules/.pnpm/@msgpack+msgpack@3.1.3/node_modules/@msgpack/msgpack/dist.cjs/index.cjs"(exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.decodeTimestampExtension = exports.encodeTimestampExtension = exports.decodeTimestampToTimeSpec = exports.encodeTimeSpecToTimestamp = exports.encodeDateToTimeSpec = exports.EXT_TIMESTAMP = exports.ExtData = exports.ExtensionCodec = exports.Encoder = exports.DecodeError = exports.Decoder = exports.decodeMultiStream = exports.decodeArrayStream = exports.decodeAsync = exports.decodeMulti = exports.decode = exports.encode = void 0;
    var encode_ts_1 = require_encode();
    Object.defineProperty(exports, "encode", { enumerable: true, get: function() {
      return encode_ts_1.encode;
    } });
    var decode_ts_1 = require_decode();
    Object.defineProperty(exports, "decode", { enumerable: true, get: function() {
      return decode_ts_1.decode;
    } });
    Object.defineProperty(exports, "decodeMulti", { enumerable: true, get: function() {
      return decode_ts_1.decodeMulti;
    } });
    var decodeAsync_ts_1 = require_decodeAsync();
    Object.defineProperty(exports, "decodeAsync", { enumerable: true, get: function() {
      return decodeAsync_ts_1.decodeAsync;
    } });
    Object.defineProperty(exports, "decodeArrayStream", { enumerable: true, get: function() {
      return decodeAsync_ts_1.decodeArrayStream;
    } });
    Object.defineProperty(exports, "decodeMultiStream", { enumerable: true, get: function() {
      return decodeAsync_ts_1.decodeMultiStream;
    } });
    var Decoder_ts_1 = require_Decoder();
    Object.defineProperty(exports, "Decoder", { enumerable: true, get: function() {
      return Decoder_ts_1.Decoder;
    } });
    var DecodeError_ts_1 = require_DecodeError();
    Object.defineProperty(exports, "DecodeError", { enumerable: true, get: function() {
      return DecodeError_ts_1.DecodeError;
    } });
    var Encoder_ts_1 = require_Encoder();
    Object.defineProperty(exports, "Encoder", { enumerable: true, get: function() {
      return Encoder_ts_1.Encoder;
    } });
    var ExtensionCodec_ts_1 = require_ExtensionCodec();
    Object.defineProperty(exports, "ExtensionCodec", { enumerable: true, get: function() {
      return ExtensionCodec_ts_1.ExtensionCodec;
    } });
    var ExtData_ts_1 = require_ExtData();
    Object.defineProperty(exports, "ExtData", { enumerable: true, get: function() {
      return ExtData_ts_1.ExtData;
    } });
    var timestamp_ts_1 = require_timestamp();
    Object.defineProperty(exports, "EXT_TIMESTAMP", { enumerable: true, get: function() {
      return timestamp_ts_1.EXT_TIMESTAMP;
    } });
    Object.defineProperty(exports, "encodeDateToTimeSpec", { enumerable: true, get: function() {
      return timestamp_ts_1.encodeDateToTimeSpec;
    } });
    Object.defineProperty(exports, "encodeTimeSpecToTimestamp", { enumerable: true, get: function() {
      return timestamp_ts_1.encodeTimeSpecToTimestamp;
    } });
    Object.defineProperty(exports, "decodeTimestampToTimeSpec", { enumerable: true, get: function() {
      return timestamp_ts_1.decodeTimestampToTimeSpec;
    } });
    Object.defineProperty(exports, "encodeTimestampExtension", { enumerable: true, get: function() {
      return timestamp_ts_1.encodeTimestampExtension;
    } });
    Object.defineProperty(exports, "decodeTimestampExtension", { enumerable: true, get: function() {
      return timestamp_ts_1.decodeTimestampExtension;
    } });
  }
});

// src/ipc/frame.ts
function encodeFrame(payload) {
  if (payload.length > MAX_PAYLOAD_SIZE) {
    throw new FrameSizeError(payload.length, MAX_PAYLOAD_SIZE);
  }
  const frame = Buffer.allocUnsafe(LENGTH_PREFIX_SIZE + payload.length);
  frame.writeUInt32BE(payload.length, 0);
  frame.set(payload, LENGTH_PREFIX_SIZE);
  return frame;
}
function encodeEventFrame(envelope) {
  const payload = (0, import_msgpack.encode)(envelope);
  return encodeFrame(payload);
}
function encodeArtifactChunkFrame(artifactId, seq, isLast, data) {
  if (seq < 1) {
    throw new ChunkValidationError(`seq must be >= 1, got ${seq}`);
  }
  if (data.length > MAX_CHUNK_SIZE) {
    throw new ChunkValidationError(
      `data size ${data.length} exceeds MAX_CHUNK_SIZE ${MAX_CHUNK_SIZE}`
    );
  }
  const frame = {
    type: "artifact_chunk",
    artifact_id: artifactId,
    seq,
    is_last: isLast,
    data
  };
  const payload = (0, import_msgpack.encode)(frame);
  return encodeFrame(payload);
}
function calculateChunks(totalSize) {
  const chunks = [];
  if (totalSize === 0) {
    chunks.push({ seq: 1, isLast: true, offset: 0, length: 0 });
    return chunks;
  }
  let offset = 0;
  let seq = 1;
  while (offset < totalSize) {
    const remaining = totalSize - offset;
    const length = Math.min(remaining, MAX_CHUNK_SIZE);
    const isLast = offset + length >= totalSize;
    chunks.push({ seq, isLast, offset, length });
    offset += length;
    seq++;
  }
  return chunks;
}
function* encodeArtifactChunks(artifactId, data) {
  const chunks = calculateChunks(data.length);
  for (const chunk of chunks) {
    const chunkData = data.subarray(chunk.offset, chunk.offset + chunk.length);
    yield encodeArtifactChunkFrame(artifactId, chunk.seq, chunk.isLast, chunkData);
  }
}
function encodeRunResultFrame(outcome, proxyUsed) {
  const frame = {
    type: "run_result",
    outcome,
    ...proxyUsed && { proxy_used: proxyUsed }
  };
  const payload = (0, import_msgpack.encode)(frame);
  return encodeFrame(payload);
}
function encodeFileWriteFrame(filename, contentType, data, writeId = 0) {
  if (data.length > MAX_CHUNK_SIZE) {
    throw new ChunkValidationError(
      `file data size ${data.length} exceeds MAX_CHUNK_SIZE ${MAX_CHUNK_SIZE}`
    );
  }
  const frame = {
    type: "file_write",
    write_id: writeId,
    filename,
    content_type: contentType,
    data
  };
  const payload = (0, import_msgpack.encode)(frame);
  return encodeFrame(payload);
}
function decodeFileWriteAck(payload) {
  const decoded = (0, import_msgpack.decode)(payload);
  if (decoded.type !== "file_write_ack") {
    throw new Error(`Expected file_write_ack frame, got type: ${String(decoded.type)}`);
  }
  return {
    type: "file_write_ack",
    write_id: decoded.write_id,
    ok: decoded.ok,
    ...decoded.error != null && { error: decoded.error }
  };
}
var import_msgpack, MAX_FRAME_SIZE, MAX_PAYLOAD_SIZE, MAX_CHUNK_SIZE, LENGTH_PREFIX_SIZE, FrameSizeError, ChunkValidationError;
var init_frame = __esm({
  "src/ipc/frame.ts"() {
    "use strict";
    import_msgpack = __toESM(require_dist(), 1);
    MAX_FRAME_SIZE = 16 * 1024 * 1024;
    MAX_PAYLOAD_SIZE = MAX_FRAME_SIZE - 4;
    MAX_CHUNK_SIZE = 8 * 1024 * 1024;
    LENGTH_PREFIX_SIZE = 4;
    FrameSizeError = class extends Error {
      constructor(payloadSize, maxPayloadSize) {
        super(`Payload size ${payloadSize} exceeds maximum ${maxPayloadSize}`);
        this.payloadSize = payloadSize;
        this.maxPayloadSize = maxPayloadSize;
        this.name = "FrameSizeError";
      }
      payloadSize;
      maxPayloadSize;
    };
    ChunkValidationError = class extends Error {
      constructor(message) {
        super(message);
        this.name = "ChunkValidationError";
      }
    };
  }
});

// src/ipc/sink.ts
function writeWithBackpressure(stream, data, writeFn) {
  return new Promise((resolve3, reject) => {
    if (stream.destroyed) {
      reject(new StreamClosedError("destroyed"));
      return;
    }
    if (stream.writableEnded || stream.writableFinished) {
      reject(new StreamClosedError("ended"));
      return;
    }
    let settled = false;
    const settle = (fn) => {
      if (settled) return;
      settled = true;
      cleanup();
      fn();
    };
    const onError = (err) => settle(() => reject(err));
    const onClose = () => settle(() => reject(new StreamClosedError("close")));
    const onFinish = () => settle(() => reject(new StreamClosedError("finish")));
    const onDrain = () => settle(() => resolve3());
    const cleanup = () => {
      stream.off("error", onError);
      stream.off("close", onClose);
      stream.off("finish", onFinish);
      stream.off("drain", onDrain);
    };
    stream.on("error", onError);
    stream.on("close", onClose);
    stream.on("finish", onFinish);
    let canContinue;
    try {
      canContinue = writeFn(data);
    } catch (err) {
      cleanup();
      reject(err instanceof Error ? err : new Error(String(err)));
      return;
    }
    if (canContinue) {
      setImmediate(() => settle(() => resolve3()));
    } else {
      stream.on("drain", onDrain);
    }
  });
}
function drainStdout() {
  return new Promise((resolve3, reject) => {
    if (process.stdout.writableFinished) {
      resolve3();
      return;
    }
    const onError = (err) => {
      cleanup();
      reject(err);
    };
    const cleanup = () => {
      process.stdout.off("error", onError);
    };
    process.stdout.on("error", onError);
    process.stdout.end(() => {
      cleanup();
      resolve3();
    });
  });
}
var StreamClosedError, StdioSink;
var init_sink = __esm({
  "src/ipc/sink.ts"() {
    "use strict";
    init_frame();
    StreamClosedError = class extends Error {
      constructor(reason) {
        super(`Output stream unavailable: ${reason}`);
        this.name = "StreamClosedError";
      }
    };
    StdioSink = class {
      /**
       * @param output - The writable stream (used for state checks and event listening)
       * @param writeFn - Optional function for actual writes. When omitted, defaults to
       *   `output.write()`. When provided, allows the caller to bypass a patched
       *   `output.write` (e.g. the stdout guard) while still using `output` for
       *   backpressure events and stream state.
       * @param ackReader - Optional AckReader for file_write_ack correlation.
       *   When provided, writeFile blocks until the runtime sends an ack.
       *   When omitted, writeFile is fire-and-forget (backward compat).
       */
      constructor(output, writeFn, ackReader) {
        this.output = output;
        this.writeFn = writeFn ?? ((data) => output.write(data));
        this.ackReader = ackReader;
      }
      output;
      writeFn;
      ackReader;
      writeIdCounter = 0;
      /**
       * Write an event envelope as a framed message.
       * Blocks on backpressure per CONTRACT_IPC.md.
       */
      async writeEvent(envelope) {
        const frame = encodeEventFrame(envelope);
        await writeWithBackpressure(this.output, frame, this.writeFn);
      }
      /**
       * Write artifact binary data as chunked frames.
       * Per CONTRACT_IPC.md, bytes are written BEFORE the artifact event.
       * Blocks on backpressure per CONTRACT_IPC.md.
       */
      async writeArtifactData(artifactId, data) {
        for (const frame of encodeArtifactChunks(artifactId, data)) {
          await writeWithBackpressure(this.output, frame, this.writeFn);
        }
      }
      /**
       * Write a run result control frame.
       * Per CONTRACT_IPC.md, this is a control frame emitted once after terminal event.
       * It does NOT affect seq ordering.
       *
       * @param outcome - The run outcome
       * @param proxyUsed - Optional redacted proxy endpoint (no password)
       */
      async writeRunResult(outcome, proxyUsed) {
        const frame = encodeRunResultFrame(outcome, proxyUsed);
        await writeWithBackpressure(this.output, frame, this.writeFn);
      }
      /**
       * Write a sidecar file via file_write frame.
       * Bypasses seq numbering and the policy pipeline.
       * Blocks on backpressure per CONTRACT_IPC.md.
       *
       * When an AckReader is configured, this method blocks until the runtime
       * sends a file_write_ack. On error ack, the returned promise rejects.
       * Without an AckReader, this is fire-and-forget (backward compat).
       *
       * @param filename - Target filename (no path separators, no "..")
       * @param contentType - MIME content type
       * @param data - Raw binary data (max 8 MiB)
       */
      async writeFile(filename, contentType, data) {
        if (this.ackReader) {
          const writeId = ++this.writeIdCounter;
          const ackPromise = this.ackReader.waitForAck(writeId);
          const frame = encodeFileWriteFrame(filename, contentType, data, writeId);
          await writeWithBackpressure(this.output, frame, this.writeFn);
          await ackPromise;
        } else {
          const frame = encodeFileWriteFrame(filename, contentType, data);
          await writeWithBackpressure(this.output, frame, this.writeFn);
        }
      }
    };
  }
});

// src/loader.ts
import { isAbsolute, resolve } from "node:path";
import { pathToFileURL } from "node:url";
function isFunction(value) {
  return typeof value === "function";
}
function isOptionalFunction(value) {
  return value === void 0 || isFunction(value);
}
function annotateImportError(message) {
  if (/needs an import attribute of.*type:\s*["']?json/i.test(message)) {
    return message + "\n\nHint: Node ESM requires an import attribute for JSON modules. Change the import to:\n  import data from './file.json' with { type: 'json' }\nThis applies to your script and all of its transitive dependencies.";
  }
  return message;
}
async function loadScript(scriptPath) {
  const absolutePath = isAbsolute(scriptPath) ? scriptPath : resolve(process.cwd(), scriptPath);
  const fileUrl = pathToFileURL(absolutePath).href;
  let module;
  try {
    module = await import(fileUrl);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    throw new ScriptLoadError(scriptPath, `import failed: ${annotateImportError(message)}`);
  }
  if (module === null || typeof module !== "object") {
    throw new ScriptLoadError(scriptPath, "module is not an object");
  }
  const mod = module;
  if (!("default" in mod)) {
    throw new ScriptLoadError(scriptPath, "missing default export");
  }
  if (!isFunction(mod.default)) {
    throw new ScriptLoadError(scriptPath, "default export is not a function");
  }
  const HOOK_NAMES2 = [
    "prepare",
    "beforeRun",
    "afterRun",
    "onError",
    "beforeTerminal",
    "cleanup"
  ];
  for (const name of HOOK_NAMES2) {
    if (!isOptionalFunction(mod[name])) {
      throw new ScriptLoadError(scriptPath, `${name} hook is not a function`);
    }
  }
  const validatedModule = mod;
  return {
    script: validatedModule.default,
    hooks: {
      prepare: validatedModule.prepare,
      beforeRun: validatedModule.beforeRun,
      afterRun: validatedModule.afterRun,
      onError: validatedModule.onError,
      beforeTerminal: validatedModule.beforeTerminal,
      cleanup: validatedModule.cleanup
    },
    module: validatedModule
  };
}
var ScriptLoadError;
var init_loader = __esm({
  "src/loader.ts"() {
    "use strict";
    ScriptLoadError = class extends Error {
      constructor(scriptPath, reason) {
        super(`Failed to load script "${scriptPath}": ${reason}`);
        this.scriptPath = scriptPath;
        this.reason = reason;
        this.name = "ScriptLoadError";
      }
      scriptPath;
      reason;
    };
  }
});

// src/executor.ts
var executor_exports = {};
__export(executor_exports, {
  _resetPuppeteerForTesting: () => _resetPuppeteerForTesting,
  errorMessage: () => errorMessage,
  execute: () => execute,
  getPuppeteer: () => getPuppeteer,
  parseRunMeta: () => parseRunMeta
});
import { createRequire } from "node:module";
import { dirname, resolve as resolve2 } from "node:path";
async function resolveModule(name, scriptPath) {
  const absoluteScriptPath = resolve2(scriptPath);
  try {
    const require2 = createRequire(absoluteScriptPath);
    const resolved = require2.resolve(name);
    return await import(resolved);
  } catch {
  }
  return await import(name);
}
function errorMessage(err) {
  return err instanceof Error ? err.message : String(err);
}
async function resolveModuleOrThrow(name, scriptPath, hint) {
  try {
    return await resolveModule(name, scriptPath);
  } catch (err) {
    const scriptDir = dirname(resolve2(scriptPath));
    throw new Error(
      `Failed to load ${name}: ${errorMessage(err)}
${hint}
Install it in your project (${scriptDir}): npm install ${name}`
    );
  }
}
async function getPuppeteer(scriptPath, plugins) {
  if (puppeteerModule && cachedPluginConfig) {
    const configChanged = cachedPluginConfig.stealth !== plugins.stealth || cachedPluginConfig.adblocker !== plugins.adblocker;
    if (!configChanged) return puppeteerModule;
    puppeteerModule = null;
    cachedPluginConfig = null;
  }
  const puppeteerMod = await resolveModuleOrThrow(
    "puppeteer",
    scriptPath,
    "Puppeteer is a peer dependency of quarry-executor."
  );
  const vanillaPuppeteer = puppeteerMod.default;
  const extraMod = await resolveModuleOrThrow(
    "puppeteer-extra",
    scriptPath,
    "puppeteer-extra is a peer dependency of quarry-executor."
  );
  const { addExtra } = extraMod;
  const pptr = addExtra(vanillaPuppeteer);
  if (plugins.stealth) {
    const stealthMod = await resolveModuleOrThrow(
      "puppeteer-extra-plugin-stealth",
      scriptPath,
      "Stealth is enabled by default.\nOr disable stealth with QUARRY_STEALTH=0"
    );
    const StealthPlugin = stealthMod.default;
    pptr.use(StealthPlugin());
  }
  if (plugins.adblocker) {
    const adblockerMod = await resolveModuleOrThrow(
      "puppeteer-extra-plugin-adblocker",
      scriptPath,
      "Adblocker was enabled but the plugin is not installed."
    );
    const AdblockerPlugin = adblockerMod.default;
    pptr.use(AdblockerPlugin({ blockTrackers: true }));
  }
  cachedPluginConfig = plugins;
  puppeteerModule = pptr;
  return pptr;
}
function _resetPuppeteerForTesting() {
  puppeteerModule = null;
  cachedPluginConfig = null;
}
async function getVanillaPuppeteer(scriptPath) {
  const puppeteerMod = await resolveModuleOrThrow(
    "puppeteer",
    scriptPath,
    "Puppeteer is a peer dependency of quarry-executor."
  );
  return puppeteerMod.default;
}
function parseRunMeta(input) {
  if (input === null || typeof input !== "object") {
    throw new Error("run metadata must be an object");
  }
  const obj = input;
  if (typeof obj.run_id !== "string" || obj.run_id === "") {
    throw new Error("run_id must be a non-empty string");
  }
  if (typeof obj.attempt !== "number" || !Number.isInteger(obj.attempt) || obj.attempt < 1) {
    throw new Error("attempt must be a positive integer");
  }
  const hasParentRunId = typeof obj.parent_run_id === "string" && obj.parent_run_id !== "";
  if (obj.attempt === 1 && hasParentRunId) {
    throw new Error("initial run (attempt=1) must not have parent_run_id");
  }
  if (obj.attempt > 1 && !hasParentRunId) {
    throw new Error(`retry run (attempt=${obj.attempt}) must have parent_run_id`);
  }
  const run = {
    run_id: obj.run_id,
    attempt: obj.attempt,
    ...typeof obj.job_id === "string" && obj.job_id !== "" && { job_id: obj.job_id },
    ...hasParentRunId && { parent_run_id: obj.parent_run_id }
  };
  return run;
}
function buildPuppeteerLaunchOptions(baseOptions, proxy) {
  if (!proxy) {
    return baseOptions ?? {};
  }
  const proxyUrl = `${proxy.protocol}://${proxy.host}:${proxy.port}`;
  const existingArgs = baseOptions?.args ?? [];
  const proxyArgs = [`--proxy-server=${proxyUrl}`];
  return {
    ...baseOptions,
    args: [...existingArgs, ...proxyArgs]
  };
}
async function emitRunResult(stdioSink, outcome, proxy) {
  try {
    const runResultOutcome = toRunResultOutcome(outcome);
    const proxyUsed = proxy ? redactProxy(proxy) : void 0;
    await stdioSink.writeRunResult(runResultOutcome, proxyUsed);
  } catch {
  }
}
async function safeClose(resource) {
  if (!resource) return;
  try {
    await resource.close();
  } catch {
  }
}
function isSinkFailure(err) {
  return !(err instanceof TerminalEventError);
}
function toRunResultOutcome(outcome) {
  switch (outcome.status) {
    case "completed":
      return {
        status: "completed",
        message: outcome.summary ? "run completed with summary" : "run completed"
      };
    case "error":
      return {
        status: "error",
        message: outcome.message,
        error_type: outcome.errorType,
        stack: outcome.stack
      };
    case "crash":
      return {
        status: "crash",
        message: outcome.message
      };
  }
}
function redactProxy(proxy) {
  return {
    protocol: proxy.protocol,
    host: proxy.host,
    port: proxy.port,
    ...proxy.username && { username: proxy.username }
  };
}
async function execute(config) {
  const output = config.output ?? process.stdout;
  const stdioSink = new StdioSink(output, config.outputWrite, config.ackReader);
  const sink = new ObservingSink(stdioSink);
  let browser = null;
  let browserContext = null;
  let page = null;
  let script = null;
  let ctx = null;
  let scriptThrew = false;
  let scriptError = null;
  let isConnected = false;
  function determineOutcome(sinkState) {
    if (sinkState.isSinkFailed()) {
      const failure = sinkState.getSinkFailure();
      const message = errorMessage(failure);
      return {
        outcome: { status: "crash", message },
        terminalEmitted: sinkState.getTerminalState() !== null
      };
    }
    const terminalState = sinkState.getTerminalState();
    if (terminalState !== null) {
      if (terminalState.type === "run_error") {
        return {
          outcome: {
            status: "error",
            errorType: terminalState.errorType ?? "unknown",
            message: terminalState.message ?? "Unknown error"
          },
          terminalEmitted: true
        };
      }
      return {
        outcome: { status: "completed", summary: terminalState.summary },
        terminalEmitted: true
      };
    }
    if (scriptThrew && scriptError) {
      return {
        outcome: {
          status: "error",
          errorType: "script_error",
          message: scriptError.message,
          stack: scriptError.stack
        },
        terminalEmitted: false
      };
    }
    return {
      outcome: { status: "completed" },
      terminalEmitted: false
    };
  }
  try {
    try {
      script = await loadScript(config.scriptPath);
    } catch (err) {
      if (err instanceof ScriptLoadError) {
        return {
          outcome: { status: "crash", message: err.message },
          terminalEmitted: false
        };
      }
      throw err;
    }
    let effectiveJob = config.job;
    if (script.hooks.prepare) {
      let prepareResult;
      try {
        prepareResult = await script.hooks.prepare(config.job, config.run);
      } catch (err) {
        const message = errorMessage(err);
        const crashOutcome = { status: "crash", message };
        await emitRunResult(stdioSink, crashOutcome, config.proxy);
        return { outcome: crashOutcome, terminalEmitted: false };
      }
      if (prepareResult === null || prepareResult === void 0 || typeof prepareResult !== "object" || !("action" in prepareResult)) {
        const crashOutcome = {
          status: "crash",
          message: `prepare hook must return { action: 'continue' | 'skip' }, got: ${String(prepareResult)}`
        };
        await emitRunResult(stdioSink, crashOutcome, config.proxy);
        return { outcome: crashOutcome, terminalEmitted: false };
      }
      if (prepareResult.action === "skip") {
        const { emit } = createAPIs(config.run, sink, config.storagePartition);
        const summary = { skipped: true };
        if (prepareResult.reason !== void 0) {
          summary.reason = prepareResult.reason;
        }
        try {
          await emit.runComplete({ summary });
        } catch {
        }
        const result2 = determineOutcome(sink);
        await emitRunResult(stdioSink, result2.outcome, config.proxy);
        return result2;
      }
      if (prepareResult.action === "continue") {
        if (prepareResult.job !== void 0) {
          effectiveJob = prepareResult.job;
        }
      } else {
        const crashOutcome = {
          status: "crash",
          message: `prepare hook returned unrecognized action: ${prepareResult.action}`
        };
        await emitRunResult(stdioSink, crashOutcome, config.proxy);
        return { outcome: crashOutcome, terminalEmitted: false };
      }
    }
    if (config.browserWSEndpoint) {
      const puppeteer = await getVanillaPuppeteer(config.scriptPath);
      browser = await puppeteer.connect({ browserWSEndpoint: config.browserWSEndpoint });
      isConnected = true;
    } else {
      const plugins = {
        stealth: config.stealth !== false,
        adblocker: config.adblocker === true
      };
      const puppeteer = await getPuppeteer(config.scriptPath, plugins);
      const launchOptions = buildPuppeteerLaunchOptions(config.puppeteerOptions, config.proxy);
      browser = await puppeteer.launch(launchOptions);
    }
    browserContext = await browser.createBrowserContext();
    page = await browserContext.newPage();
    if (config.proxy?.username && config.proxy?.password) {
      await page.authenticate({
        username: config.proxy.username,
        password: config.proxy.password
      });
    }
    ctx = createContext({
      job: effectiveJob,
      run: config.run,
      page,
      browser,
      browserContext,
      sink,
      storagePartition: config.storagePartition
    });
    let rawScriptError = null;
    try {
      if (script.hooks.beforeRun) {
        await script.hooks.beforeRun(ctx);
      }
      await script.script(ctx);
      if (script.hooks.afterRun) {
        await script.hooks.afterRun(ctx);
      }
    } catch (err) {
      scriptThrew = true;
      rawScriptError = err;
      scriptError = {
        message: errorMessage(err),
        stack: err instanceof Error ? err.stack : void 0
      };
      if (script.hooks.onError && sink.getTerminalState() === null) {
        try {
          await script.hooks.onError(err, ctx);
        } catch {
        }
      }
    }
    if (script.hooks.beforeTerminal && !sink.isSinkFailed() && sink.getTerminalState() === null) {
      const signal = rawScriptError !== null ? { outcome: "error", error: rawScriptError } : { outcome: "completed" };
      try {
        await script.hooks.beforeTerminal(signal, ctx);
      } catch {
      }
    }
    if (!sink.isSinkFailed() && sink.getTerminalState() === null) {
      try {
        if (scriptThrew && scriptError) {
          await ctx.emit.runError({
            error_type: "script_error",
            message: scriptError.message,
            stack: scriptError.stack
          });
        } else {
          await ctx.emit.runComplete();
        }
      } catch (err) {
        if (isSinkFailure(err)) {
        }
      }
    }
    if (script.hooks.cleanup && ctx) {
      try {
        await script.hooks.cleanup(ctx);
      } catch {
      }
    }
    const result = determineOutcome(sink);
    await emitRunResult(stdioSink, result.outcome, config.proxy);
    return result;
  } catch (err) {
    const message = errorMessage(err);
    const crashOutcome = { status: "crash", message };
    await emitRunResult(stdioSink, crashOutcome, config.proxy);
    return {
      outcome: crashOutcome,
      terminalEmitted: false
    };
  } finally {
    await safeClose(page);
    await safeClose(browserContext);
    if (isConnected && browser) {
      try {
        browser.disconnect();
      } catch {
      }
    } else {
      await safeClose(browser);
    }
  }
}
var puppeteerModule, cachedPluginConfig;
var init_executor = __esm({
  "src/executor.ts"() {
    "use strict";
    init_dist();
    init_observing_sink();
    init_sink();
    init_loader();
    puppeteerModule = null;
    cachedPluginConfig = null;
  }
});

// src/resolve-from.ts
var resolve_from_exports = {};
__export(resolve_from_exports, {
  registerResolveFromHook: () => registerResolveFromHook,
  resolveFromHookCode: () => resolveFromHookCode
});
async function registerResolveFromHook(resolveFrom) {
  const { register } = await import("node:module");
  const hookUrl = `data:text/javascript;base64,${Buffer.from(resolveFromHookCode).toString("base64")}`;
  register(hookUrl, { data: { resolveFrom } });
}
var resolveFromHookCode;
var init_resolve_from = __esm({
  "src/resolve-from.ts"() {
    "use strict";
    resolveFromHookCode = `
  import { pathToFileURL } from 'node:url';

  let fallbackParentURL;

  export function initialize(data) {
    // Construct a file:// URL pointing into the resolve-from directory.
    // nextResolve uses parentURL as the resolution base, so ESM
    // "imports" conditions are applied (not CJS "require" conditions).
    fallbackParentURL = pathToFileURL(data.resolveFrom + '/noop.js').href;
  }

  export async function resolve(specifier, context, nextResolve) {
    // Skip relative and absolute specifiers \u2014 only intercept bare specifiers
    if (specifier.startsWith('.') || specifier.startsWith('/') || specifier.startsWith('file:')) {
      return nextResolve(specifier, context);
    }

    // Try default resolution first
    try {
      return await nextResolve(specifier, context);
    } catch (defaultErr) {
      // Retry with parentURL pointing at --resolve-from directory.
      // This preserves ESM import conditions (package.json "exports"
      // with "import" key) unlike CJS require-based resolution.
      try {
        const result = await nextResolve(specifier, {
          ...context,
          parentURL: fallbackParentURL,
        });
        process.stderr.write(
          'quarry: resolved "' + specifier + '" via --resolve-from fallback\\n'
        );
        return result;
      } catch {
        // Re-throw the original error if fallback also fails
        throw defaultErr;
      }
    }
  }
`;
  }
});

// src/bin/executor.ts
import { unlinkSync } from "node:fs";

// src/browser-args.ts
function chromiumArgs(extra = []) {
  const args = ["--disable-dev-shm-usage", ...extra];
  if (process.env.QUARRY_NO_SANDBOX === "1") {
    args.push("--no-sandbox", "--disable-setuid-sandbox");
  }
  return args;
}

// src/browser-idle.ts
function evaluateIdlePoll(fetchResult, state, config, now = Date.now()) {
  if (!fetchResult.ok) {
    const failures = state.consecutiveFailures + 1;
    if (failures >= config.maxConsecutiveFailures) {
      return { type: "crash-exit", failures };
    }
    return {
      type: "continue",
      nextState: { ...state, consecutiveFailures: failures }
    };
  }
  if (fetchResult.activePages > 0) {
    return {
      type: "continue",
      nextState: { idleStartedAt: null, consecutiveFailures: 0 }
    };
  }
  const idleStartedAt = state.idleStartedAt ?? now;
  if (now - idleStartedAt >= config.idleTimeoutMs) {
    return { type: "shutdown" };
  }
  return {
    type: "continue",
    nextState: { idleStartedAt, consecutiveFailures: 0 }
  };
}

// src/bin/executor.ts
init_executor();

// src/ipc/ack-reader.ts
init_frame();
var LENGTH_PREFIX_SIZE2 = 4;
var AckReader = class {
  pending = /* @__PURE__ */ new Map();
  buffer = Buffer.alloc(0);
  stopped = false;
  /** True when the runtime does not support ack frames (fire-and-forget fallback). */
  noAckSupport = false;
  /** True after at least one ack frame has been successfully dispatched. */
  receivedAnyAck = false;
  stream;
  constructor(stream) {
    this.stream = stream;
  }
  /**
   * Register a pending ack for the given writeId.
   * Returns a promise that resolves on success ack or rejects on error ack/EOF.
   */
  waitForAck(writeId) {
    if (this.noAckSupport) {
      return Promise.resolve();
    }
    if (this.stopped) {
      return Promise.reject(new Error("AckReader is stopped"));
    }
    return new Promise((resolve3, reject) => {
      this.pending.set(writeId, { resolve: resolve3, reject });
    });
  }
  /**
   * Returns true if the runtime does not support ack frames.
   * Detected when stdin EOF arrives without having received any ack frames.
   */
  get hasAckSupport() {
    return !this.noAckSupport;
  }
  /**
   * Start reading frames from the stream.
   * Attaches data/end/error listeners.
   */
  start() {
    this.stream.on("data", this.onData);
    this.stream.on("end", this.onEnd);
    this.stream.on("error", this.onError);
  }
  /**
   * Stop the reader and reject all pending promises.
   */
  stop() {
    if (this.stopped) return;
    this.stopped = true;
    this.stream.removeListener("data", this.onData);
    this.stream.removeListener("end", this.onEnd);
    this.stream.removeListener("error", this.onError);
    this.rejectAll(new Error("AckReader stopped"));
  }
  /**
   * Returns true if there are no pending ack promises.
   */
  get idle() {
    return this.pending.size === 0;
  }
  onData = (chunk) => {
    this.buffer = Buffer.concat([this.buffer, chunk]);
    this.drainBuffer();
  };
  onEnd = () => {
    this.stopped = true;
    if (!this.receivedAnyAck) {
      this.noAckSupport = true;
      this.resolveAll();
      return;
    }
    this.rejectAll(new Error("stdin closed (EOF)"));
  };
  onError = (err) => {
    this.stopped = true;
    this.rejectAll(new Error(`stdin error: ${err.message}`));
  };
  /** Consume complete frames from the internal buffer. */
  drainBuffer() {
    while (this.buffer.length >= LENGTH_PREFIX_SIZE2) {
      const payloadLen = this.buffer.readUInt32BE(0);
      const totalLen = LENGTH_PREFIX_SIZE2 + payloadLen;
      if (this.buffer.length < totalLen) {
        return;
      }
      const payload = this.buffer.subarray(LENGTH_PREFIX_SIZE2, totalLen);
      this.buffer = this.buffer.subarray(totalLen);
      this.processPayload(payload);
    }
  }
  /** Decode and dispatch a single ack frame. */
  processPayload(payload) {
    let ack;
    try {
      ack = decodeFileWriteAck(payload);
    } catch {
      return;
    }
    const entry = this.pending.get(ack.write_id);
    if (!entry) {
      return;
    }
    this.pending.delete(ack.write_id);
    this.receivedAnyAck = true;
    if (ack.ok) {
      entry.resolve();
    } else {
      entry.reject(new Error(ack.error ?? "file write failed"));
    }
  }
  /** Resolve all pending promises (fire-and-forget fallback). */
  resolveAll() {
    for (const [, entry] of this.pending) {
      entry.resolve();
    }
    this.pending.clear();
  }
  /** Reject all pending promises with the given error. */
  rejectAll(err) {
    for (const [, entry] of this.pending) {
      entry.reject(err);
    }
    this.pending.clear();
  }
};

// src/bin/executor.ts
init_sink();

// src/ipc/stdout-guard.ts
var installed = false;
function installStdoutGuard() {
  if (installed) {
    throw new Error("installStdoutGuard() must only be called once per process");
  }
  installed = true;
  const origWrite = process.stdout.write.bind(process.stdout);
  process.stdout.write = ((chunk, encodingOrCallback, callback) => {
    const text = typeof chunk === "string" ? chunk : Buffer.from(chunk).toString("utf-8");
    const preview = text.replace(/\n/g, "\\n").slice(0, 200);
    process.stderr.write(`[quarry] stdout guard: intercepted stray write: ${preview}
`);
    if (typeof encodingOrCallback === "function") {
      process.stderr.write(chunk, encodingOrCallback);
    } else {
      process.stderr.write(chunk, encodingOrCallback, callback);
    }
    return true;
  });
  return {
    ipcOutput: process.stdout,
    ipcWrite: (data) => origWrite(data)
  };
}

// src/bin/executor.ts
init_loader();

// src/parse-storage-partition.ts
function parseStoragePartition(input, onWarning) {
  if (!("storage" in input) || input.storage === null || input.storage === void 0) {
    return void 0;
  }
  if (typeof input.storage !== "object" || Array.isArray(input.storage)) {
    onWarning(
      `storage partition metadata must be an object, got ${Array.isArray(input.storage) ? "array" : typeof input.storage}; storage.put() will return empty key`
    );
    return void 0;
  }
  const sp = input.storage;
  const requiredFields = ["dataset", "source", "category", "day", "run_id"];
  const missing = requiredFields.filter(
    (f) => typeof sp[f] !== "string" || sp[f] === ""
  );
  if (missing.length > 0) {
    onWarning(
      `storage partition metadata present but malformed (missing/empty: ${missing.join(", ")}); storage.put() will return empty key`
    );
    return void 0;
  }
  return {
    dataset: sp.dataset,
    source: sp.source,
    category: sp.category,
    day: sp.day,
    run_id: sp.run_id
  };
}

// src/bin/executor.ts
function fatalError(message) {
  process.stderr.write(`Error: ${message}
`);
  process.exit(3);
}
var HOOK_NAMES = [
  "prepare",
  "beforeRun",
  "afterRun",
  "onError",
  "beforeTerminal",
  "cleanup"
];
async function validateScript(scriptPath) {
  const resolveFrom = process.env.QUARRY_RESOLVE_FROM;
  if (resolveFrom) {
    const { registerResolveFromHook: registerResolveFromHook2 } = await Promise.resolve().then(() => (init_resolve_from(), resolve_from_exports));
    await registerResolveFromHook2(resolveFrom);
  }
  let loaded;
  try {
    loaded = await loadScript(scriptPath);
  } catch (err) {
    const message = err instanceof ScriptLoadError ? err.message : errorMessage(err);
    const result2 = { valid: false, error: message };
    process.stdout.write(JSON.stringify(result2) + "\n");
    process.exit(1);
  }
  const hooks = [];
  for (const name of HOOK_NAMES) {
    if (loaded.hooks[name] !== void 0) {
      hooks.push(name);
    }
  }
  const result = {
    valid: true,
    exports: {
      default: true,
      hooks
    }
  };
  process.stdout.write(JSON.stringify(result) + "\n");
  process.exit(0);
}
function parseProxy(input) {
  if (!("proxy" in input) || input.proxy === null || input.proxy === void 0) {
    return void 0;
  }
  const proxy = input.proxy;
  if (typeof proxy.protocol !== "string") {
    throw new Error("proxy.protocol must be a string");
  }
  if (typeof proxy.host !== "string" || proxy.host === "") {
    throw new Error("proxy.host must be a non-empty string");
  }
  if (typeof proxy.port !== "number" || !Number.isInteger(proxy.port) || proxy.port < 1 || proxy.port > 65535) {
    throw new Error("proxy.port must be an integer between 1 and 65535");
  }
  const validProtocols = ["http", "https", "socks5"];
  if (!validProtocols.includes(proxy.protocol)) {
    throw new Error(`proxy.protocol must be one of: ${validProtocols.join(", ")}`);
  }
  const hasUsername = typeof proxy.username === "string" && proxy.username !== "";
  const hasPassword = typeof proxy.password === "string" && proxy.password !== "";
  if (hasUsername !== hasPassword) {
    throw new Error("proxy.username and proxy.password must be provided together");
  }
  return {
    protocol: proxy.protocol,
    host: proxy.host,
    port: proxy.port,
    ...hasUsername && { username: proxy.username },
    ...hasPassword && { password: proxy.password }
  };
}
async function readStdinMetadata() {
  return new Promise((resolve3, reject) => {
    const chunks = [];
    function onReadable() {
      let chunk;
      while ((chunk = process.stdin.read()) !== null) {
        const nlIndex = chunk.indexOf(10);
        if (nlIndex !== -1) {
          chunks.push(chunk.subarray(0, nlIndex));
          const remaining = chunk.subarray(nlIndex + 1);
          if (remaining.length > 0) {
            process.stdin.unshift(remaining);
          }
          cleanup();
          resolve3(Buffer.concat(chunks).toString("utf-8"));
          return;
        }
        chunks.push(chunk);
      }
    }
    function onEnd() {
      cleanup();
      resolve3(Buffer.concat(chunks).toString("utf-8"));
    }
    function onError(err) {
      cleanup();
      reject(err);
    }
    function cleanup() {
      process.stdin.removeListener("readable", onReadable);
      process.stdin.removeListener("end", onEnd);
      process.stdin.removeListener("error", onError);
    }
    process.stdin.on("readable", onReadable);
    process.stdin.on("end", onEnd);
    process.stdin.on("error", onError);
  });
}
async function browserServer(scriptPath) {
  const { getPuppeteer: getBrowserPuppeteer } = await Promise.resolve().then(() => (init_executor(), executor_exports));
  const puppeteer = await getBrowserPuppeteer(scriptPath, {
    stealth: process.env.QUARRY_STEALTH !== "0",
    adblocker: process.env.QUARRY_ADBLOCKER === "1"
  });
  const proxyUrl = process.env.QUARRY_BROWSER_PROXY;
  const proxyArgs = proxyUrl ? [`--proxy-server=${proxyUrl}`] : [];
  const browser = await puppeteer.launch({
    headless: true,
    args: chromiumArgs(proxyArgs)
  });
  const wsEndpoint = browser.wsEndpoint();
  process.stdout.write(`${wsEndpoint}
`);
  process.on("SIGPIPE", () => {
  });
  const idleTimeoutMs = Number.parseInt(process.env.QUARRY_BROWSER_IDLE_TIMEOUT ?? "60", 10) * 1e3;
  const discoveryFile = process.env.QUARRY_BROWSER_DISCOVERY_FILE ?? "";
  const wsUrl = new URL(wsEndpoint);
  const baseUrl = `http://127.0.0.1:${wsUrl.port}`;
  const pollIntervalMs = 5e3;
  async function countActivePages() {
    const res = await fetch(`${baseUrl}/json/list`);
    if (!res.ok) throw new Error(`/json/list returned ${res.status}`);
    const targets = await res.json();
    return targets.filter((t) => t.type === "page" && t.url !== "about:blank").length;
  }
  function removeDiscoveryFile() {
    if (!discoveryFile) return;
    try {
      unlinkSync(discoveryFile);
    } catch {
    }
  }
  const pollConfig = { idleTimeoutMs, maxConsecutiveFailures: 3 };
  let pollState = { idleStartedAt: null, consecutiveFailures: 0 };
  async function shutdown() {
    const idleSec = pollState.idleStartedAt ? Math.round((Date.now() - pollState.idleStartedAt) / 1e3) : 0;
    process.stderr.write(`Browser server idle for ${idleSec}s, shutting down
`);
    removeDiscoveryFile();
    await browser.close();
    process.exit(0);
  }
  process.on("SIGTERM", () => void shutdown());
  process.on("SIGINT", () => void shutdown());
  const timer = setInterval(async () => {
    let fetchResult;
    try {
      fetchResult = { ok: true, activePages: await countActivePages() };
    } catch {
      fetchResult = { ok: false };
    }
    const action = evaluateIdlePoll(fetchResult, pollState, pollConfig);
    switch (action.type) {
      case "continue":
        pollState = action.nextState;
        return;
      case "shutdown":
        clearInterval(timer);
        await shutdown();
        return;
      case "crash-exit":
        process.stderr.write(
          `Browser health check failed ${action.failures} times consecutively, exiting
`
        );
        clearInterval(timer);
        removeDiscoveryFile();
        process.exit(1);
    }
  }, pollIntervalMs);
  while (true) {
    await new Promise((resolve3) => setTimeout(resolve3, 2147483647));
  }
}
async function launchBrowserServer(scriptPath) {
  const { getPuppeteer: getBrowserPuppeteer } = await Promise.resolve().then(() => (init_executor(), executor_exports));
  const puppeteer = await getBrowserPuppeteer(scriptPath, {
    stealth: process.env.QUARRY_STEALTH !== "0",
    adblocker: process.env.QUARRY_ADBLOCKER === "1"
  });
  const browser = await puppeteer.launch({
    headless: true,
    args: chromiumArgs()
  });
  const wsEndpoint = browser.wsEndpoint();
  process.stdout.write(`${wsEndpoint}
`);
  await new Promise((resolve3) => {
    process.stdin.resume();
    process.stdin.on("end", resolve3);
    process.stdin.on("close", resolve3);
    process.on("SIGTERM", resolve3);
    process.on("SIGINT", resolve3);
  });
  await browser.close();
  process.exit(0);
}
async function main() {
  const args = process.argv.slice(2);
  if (args[0] === "--browser-server") {
    const scriptPath2 = args[1];
    if (!scriptPath2) {
      process.stderr.write("Usage: quarry-executor --browser-server <script-path>\n");
      process.exit(3);
    }
    return browserServer(scriptPath2);
  }
  if (args[0] === "--validate") {
    const scriptPath2 = args[1];
    if (!scriptPath2) {
      process.stderr.write("Usage: quarry-executor --validate <script-path>\n");
      process.exit(3);
    }
    return validateScript(scriptPath2);
  }
  if (args[0] === "--launch-browser") {
    const scriptPath2 = args[1];
    if (!scriptPath2) {
      process.stderr.write("Usage: quarry-executor --launch-browser <script-path>\n");
      process.exit(3);
    }
    return launchBrowserServer(scriptPath2);
  }
  if (args.length < 1) {
    process.stderr.write("Usage: quarry-executor <script-path>\n");
    process.stderr.write("Run metadata is read from stdin as JSON.\n");
    process.exit(3);
  }
  const { ipcOutput, ipcWrite } = installStdoutGuard();
  const resolveFrom = process.env.QUARRY_RESOLVE_FROM;
  if (resolveFrom) {
    const { registerResolveFromHook: registerResolveFromHook2 } = await Promise.resolve().then(() => (init_resolve_from(), resolve_from_exports));
    await registerResolveFromHook2(resolveFrom);
  }
  const scriptPath = args[0];
  let input;
  try {
    const stdinData = await readStdinMetadata();
    if (stdinData.trim() === "") {
      fatalError("stdin is empty, expected JSON input");
    }
    input = JSON.parse(stdinData);
  } catch (err) {
    fatalError(`parsing stdin JSON: ${errorMessage(err)}`);
  }
  if (input === null || typeof input !== "object") {
    fatalError("stdin must be a JSON object");
  }
  const inputObj = input;
  let run;
  try {
    run = parseRunMeta(inputObj);
  } catch (err) {
    fatalError(`parsing run metadata: ${errorMessage(err)}`);
  }
  if (!("job" in inputObj)) {
    fatalError('missing "job" field in input');
  }
  const job = inputObj.job;
  let proxy;
  try {
    proxy = parseProxy(inputObj);
  } catch (err) {
    fatalError(`parsing proxy: ${errorMessage(err)}`);
  }
  const storagePartition = parseStoragePartition(inputObj, (msg) => {
    process.stderr.write(`Warning: ${msg}
`);
  });
  const browserWSEndpoint = typeof inputObj.browser_ws_endpoint === "string" && inputObj.browser_ws_endpoint !== "" ? inputObj.browser_ws_endpoint : void 0;
  const ackReader = new AckReader(process.stdin);
  ackReader.start();
  const result = await execute({
    scriptPath,
    job,
    run,
    proxy,
    storagePartition,
    browserWSEndpoint,
    ackReader,
    output: ipcOutput,
    outputWrite: ipcWrite,
    puppeteerOptions: {
      headless: true,
      args: chromiumArgs()
    },
    // Stealth on by default; disable with QUARRY_STEALTH=0
    stealth: process.env.QUARRY_STEALTH !== "0",
    // Adblocker off by default; enable with QUARRY_ADBLOCKER=1
    adblocker: process.env.QUARRY_ADBLOCKER === "1"
  });
  ackReader.stop();
  await drainStdout();
  switch (result.outcome.status) {
    case "completed":
      process.exit(0);
      break;
    case "error":
      process.exit(1);
      break;
    case "crash":
      process.stderr.write(`Executor crash: ${result.outcome.message}
`);
      process.exit(2);
      break;
    default: {
      const _exhaustive = result.outcome;
      process.exit(2);
    }
  }
}
main().catch((err) => {
  process.stderr.write(`Unexpected error: ${errorMessage(err)}
`);
  process.exit(2);
});
