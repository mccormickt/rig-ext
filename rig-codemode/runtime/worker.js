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

// node_modules/@jitl/quickjs-ffi-types/dist/index.mjs
var EvalFlags, IntrinsicsFlags, JSPromiseStateEnum, GetOwnPropertyNamesFlags, IsEqualOp;
var init_dist = __esm({
  "node_modules/@jitl/quickjs-ffi-types/dist/index.mjs"() {
    EvalFlags = { JS_EVAL_TYPE_GLOBAL: 0, JS_EVAL_TYPE_MODULE: 1, JS_EVAL_TYPE_DIRECT: 2, JS_EVAL_TYPE_INDIRECT: 3, JS_EVAL_TYPE_MASK: 3, JS_EVAL_FLAG_STRICT: 8, JS_EVAL_FLAG_STRIP: 16, JS_EVAL_FLAG_COMPILE_ONLY: 32, JS_EVAL_FLAG_BACKTRACE_BARRIER: 64 };
    IntrinsicsFlags = { BaseObjects: 1, Date: 2, Eval: 4, StringNormalize: 8, RegExp: 16, RegExpCompiler: 32, JSON: 64, Proxy: 128, MapSet: 256, TypedArrays: 512, Promise: 1024, BigInt: 2048, BigFloat: 4096, BigDecimal: 8192, OperatorOverloading: 16384, BignumExt: 32768 };
    JSPromiseStateEnum = { Pending: 0, Fulfilled: 1, Rejected: 2 };
    GetOwnPropertyNamesFlags = { JS_GPN_STRING_MASK: 1, JS_GPN_SYMBOL_MASK: 2, JS_GPN_PRIVATE_MASK: 4, JS_GPN_ENUM_ONLY: 16, JS_GPN_SET_ENUM: 32, QTS_GPN_NUMBER_MASK: 64, QTS_STANDARD_COMPLIANT_NUMBER: 128 };
    IsEqualOp = { IsStrictlyEqual: 0, IsSameValue: 1, IsSameValueZero: 2 };
  }
});

// node_modules/quickjs-emscripten-core/dist/chunk-V2S4ZYJR.mjs
function debugLog(...args) {
  QTS_DEBUG && console.log("quickjs-emscripten:", ...args);
}
function* awaitYield(value) {
  return yield value;
}
function awaitYieldOf(generator) {
  return awaitYield(awaitEachYieldedPromise(generator));
}
function maybeAsyncFn(that, fn) {
  return (...args) => {
    let generator = fn.call(that, AwaitYield, ...args);
    return awaitEachYieldedPromise(generator);
  };
}
function maybeAsync(that, startGenerator) {
  let generator = startGenerator.call(that, AwaitYield);
  return awaitEachYieldedPromise(generator);
}
function awaitEachYieldedPromise(gen) {
  function handleNextStep(step) {
    return step.done ? step.value : step.value instanceof Promise ? step.value.then((value) => handleNextStep(gen.next(value)), (error) => handleNextStep(gen.throw(error))) : handleNextStep(gen.next(step.value));
  }
  return handleNextStep(gen.next());
}
function scopeFinally(scope, blockError) {
  let disposeError;
  try {
    scope.dispose();
  } catch (error) {
    disposeError = error;
  }
  if (blockError && disposeError) throw Object.assign(blockError, { message: `${blockError.message}
 Then, failed to dispose scope: ${disposeError.message}`, disposeError }), blockError;
  if (blockError || disposeError) throw blockError || disposeError;
}
function createDisposableArray(items) {
  let array = items ? Array.from(items) : [];
  function disposeAlive() {
    return array.forEach((disposable) => disposable.alive ? disposable.dispose() : void 0);
  }
  function someIsAlive() {
    return array.some((disposable) => disposable.alive);
  }
  return Object.defineProperty(array, SymbolDispose, { configurable: true, enumerable: false, value: disposeAlive }), Object.defineProperty(array, "dispose", { configurable: true, enumerable: false, value: disposeAlive }), Object.defineProperty(array, "alive", { configurable: true, enumerable: false, get: someIsAlive }), array;
}
function isDisposable(value) {
  return !!(value && (typeof value == "object" || typeof value == "function") && "alive" in value && typeof value.alive == "boolean" && "dispose" in value && typeof value.dispose == "function");
}
function intrinsicsToFlags(intrinsics) {
  if (!intrinsics) return 0;
  let result = 0;
  for (let [maybeIntrinsicName, enabled] of Object.entries(intrinsics)) {
    if (!(maybeIntrinsicName in IntrinsicsFlags)) throw new QuickJSUnknownIntrinsic(maybeIntrinsicName);
    enabled && (result |= IntrinsicsFlags[maybeIntrinsicName]);
  }
  return result;
}
function evalOptionsToFlags(evalOptions) {
  if (typeof evalOptions == "number") return evalOptions;
  if (evalOptions === void 0) return 0;
  let { type, strict, strip, compileOnly, backtraceBarrier } = evalOptions, flags = 0;
  return type === "global" && (flags |= EvalFlags.JS_EVAL_TYPE_GLOBAL), type === "module" && (flags |= EvalFlags.JS_EVAL_TYPE_MODULE), strict && (flags |= EvalFlags.JS_EVAL_FLAG_STRICT), strip && (flags |= EvalFlags.JS_EVAL_FLAG_STRIP), compileOnly && (flags |= EvalFlags.JS_EVAL_FLAG_COMPILE_ONLY), backtraceBarrier && (flags |= EvalFlags.JS_EVAL_FLAG_BACKTRACE_BARRIER), flags;
}
function getOwnPropertyNamesOptionsToFlags(options) {
  if (typeof options == "number") return options;
  if (options === void 0) return 0;
  let { strings: includeStrings, symbols: includeSymbols, quickjsPrivate: includePrivate, onlyEnumerable, numbers: includeNumbers, numbersAsStrings } = options, flags = 0;
  return includeStrings && (flags |= GetOwnPropertyNamesFlags.JS_GPN_STRING_MASK), includeSymbols && (flags |= GetOwnPropertyNamesFlags.JS_GPN_SYMBOL_MASK), includePrivate && (flags |= GetOwnPropertyNamesFlags.JS_GPN_PRIVATE_MASK), onlyEnumerable && (flags |= GetOwnPropertyNamesFlags.JS_GPN_ENUM_ONLY), includeNumbers && (flags |= GetOwnPropertyNamesFlags.QTS_GPN_NUMBER_MASK), numbersAsStrings && (flags |= GetOwnPropertyNamesFlags.QTS_STANDARD_COMPLIANT_NUMBER), flags;
}
function concat(...values) {
  let result = [];
  for (let value of values) value !== void 0 && (result = result.concat(value));
  return result;
}
function getGroupId(id) {
  return id >> 8;
}
function applyBaseRuntimeOptions(runtime, options) {
  options.interruptHandler && runtime.setInterruptHandler(options.interruptHandler), options.maxStackSizeBytes !== void 0 && runtime.setMaxStackSize(options.maxStackSizeBytes), options.memoryLimitBytes !== void 0 && runtime.setMemoryLimit(options.memoryLimitBytes);
}
function applyModuleEvalRuntimeOptions(runtime, options) {
  options.moduleLoader && runtime.setModuleLoader(options.moduleLoader), options.shouldInterrupt && runtime.setInterruptHandler(options.shouldInterrupt), options.memoryLimitBytes !== void 0 && runtime.setMemoryLimit(options.memoryLimitBytes), options.maxStackSizeBytes !== void 0 && runtime.setMaxStackSize(options.maxStackSizeBytes);
}
var __defProp2, __export2, QTS_DEBUG, errors_exports, QuickJSUnwrapError, QuickJSWrongOwner, QuickJSUseAfterFree, QuickJSNotImplemented, QuickJSAsyncifyError, QuickJSAsyncifySuspended, QuickJSMemoryLeakDetected, QuickJSEmscriptenModuleError, QuickJSUnknownIntrinsic, QuickJSPromisePending, QuickJSEmptyGetOwnPropertyNames, QuickJSHostRefRangeExceeded, QuickJSHostRefInvalid, AwaitYield, UsingDisposable, SymbolDispose, prototypeAsAny, Lifetime, StaticLifetime, WeakLifetime, Scope, AbstractDisposableResult, DisposableSuccess, DisposableFail, DisposableResult, QuickJSDeferredPromise, ModuleMemory, DefaultIntrinsics, QuickJSIterator, INT32_MIN, INT32_MAX, INVALID_HOST_REF_ID, HostRefMap, HostRef, ContextMemory, QuickJSContext, QuickJSRuntime, QuickJSEmscriptenModuleCallbacks, QuickJSModuleCallbacks, QuickJSWASMModule;
var init_chunk_V2S4ZYJR = __esm({
  "node_modules/quickjs-emscripten-core/dist/chunk-V2S4ZYJR.mjs"() {
    init_dist();
    init_dist();
    __defProp2 = Object.defineProperty;
    __export2 = (target, all) => {
      for (var name in all) __defProp2(target, name, { get: all[name], enumerable: true });
    };
    QTS_DEBUG = false;
    errors_exports = {};
    __export2(errors_exports, { QuickJSAsyncifyError: () => QuickJSAsyncifyError, QuickJSAsyncifySuspended: () => QuickJSAsyncifySuspended, QuickJSEmptyGetOwnPropertyNames: () => QuickJSEmptyGetOwnPropertyNames, QuickJSEmscriptenModuleError: () => QuickJSEmscriptenModuleError, QuickJSHostRefInvalid: () => QuickJSHostRefInvalid, QuickJSHostRefRangeExceeded: () => QuickJSHostRefRangeExceeded, QuickJSMemoryLeakDetected: () => QuickJSMemoryLeakDetected, QuickJSNotImplemented: () => QuickJSNotImplemented, QuickJSPromisePending: () => QuickJSPromisePending, QuickJSUnknownIntrinsic: () => QuickJSUnknownIntrinsic, QuickJSUnwrapError: () => QuickJSUnwrapError, QuickJSUseAfterFree: () => QuickJSUseAfterFree, QuickJSWrongOwner: () => QuickJSWrongOwner });
    QuickJSUnwrapError = class extends Error {
      constructor(cause, context) {
        let message = typeof cause == "object" && cause && "message" in cause ? String(cause.message) : String(cause);
        super(message);
        this.cause = cause;
        this.context = context;
        this.name = "QuickJSUnwrapError";
      }
    };
    QuickJSWrongOwner = class extends Error {
      constructor() {
        super(...arguments);
        this.name = "QuickJSWrongOwner";
      }
    };
    QuickJSUseAfterFree = class extends Error {
      constructor() {
        super(...arguments);
        this.name = "QuickJSUseAfterFree";
      }
    };
    QuickJSNotImplemented = class extends Error {
      constructor() {
        super(...arguments);
        this.name = "QuickJSNotImplemented";
      }
    };
    QuickJSAsyncifyError = class extends Error {
      constructor() {
        super(...arguments);
        this.name = "QuickJSAsyncifyError";
      }
    };
    QuickJSAsyncifySuspended = class extends Error {
      constructor() {
        super(...arguments);
        this.name = "QuickJSAsyncifySuspended";
      }
    };
    QuickJSMemoryLeakDetected = class extends Error {
      constructor() {
        super(...arguments);
        this.name = "QuickJSMemoryLeakDetected";
      }
    };
    QuickJSEmscriptenModuleError = class extends Error {
      constructor() {
        super(...arguments);
        this.name = "QuickJSEmscriptenModuleError";
      }
    };
    QuickJSUnknownIntrinsic = class extends TypeError {
      constructor() {
        super(...arguments);
        this.name = "QuickJSUnknownIntrinsic";
      }
    };
    QuickJSPromisePending = class extends Error {
      constructor() {
        super(...arguments);
        this.name = "QuickJSPromisePending";
      }
    };
    QuickJSEmptyGetOwnPropertyNames = class extends Error {
      constructor() {
        super(...arguments);
        this.name = "QuickJSEmptyGetOwnPropertyNames";
      }
    };
    QuickJSHostRefRangeExceeded = class extends Error {
      constructor() {
        super(...arguments);
        this.name = "QuickJSHostRefRangeExceeded";
      }
    };
    QuickJSHostRefInvalid = class extends Error {
      constructor() {
        super(...arguments);
        this.name = "QuickJSHostRefInvalid";
      }
    };
    AwaitYield = awaitYield;
    AwaitYield.of = awaitYieldOf;
    UsingDisposable = class {
      [Symbol.dispose]() {
        return this.dispose();
      }
    };
    SymbolDispose = Symbol.dispose ?? /* @__PURE__ */ Symbol.for("Symbol.dispose");
    prototypeAsAny = UsingDisposable.prototype;
    prototypeAsAny[SymbolDispose] || (prototypeAsAny[SymbolDispose] = function() {
      return this.dispose();
    });
    Lifetime = class _Lifetime extends UsingDisposable {
      constructor(_value, copier, disposer, _owner) {
        super();
        this._value = _value;
        this.copier = copier;
        this.disposer = disposer;
        this._owner = _owner;
        this._alive = true;
        this._constructorStack = QTS_DEBUG ? new Error("Lifetime constructed").stack : void 0;
      }
      get alive() {
        return this._alive;
      }
      get value() {
        return this.assertAlive(), this._value;
      }
      get owner() {
        return this._owner;
      }
      get dupable() {
        return !!this.copier;
      }
      dup() {
        if (this.assertAlive(), !this.copier) throw new Error("Non-dupable lifetime");
        return new _Lifetime(this.copier(this._value), this.copier, this.disposer, this._owner);
      }
      consume(map) {
        this.assertAlive();
        let result = map(this);
        return this.dispose(), result;
      }
      map(map) {
        return this.assertAlive(), map(this);
      }
      tap(fn) {
        return fn(this), this;
      }
      dispose() {
        this.assertAlive(), this.disposer && this.disposer(this._value), this._alive = false;
      }
      assertAlive() {
        if (!this.alive) throw this._constructorStack ? new QuickJSUseAfterFree(`Lifetime not alive
${this._constructorStack}
Lifetime used`) : new QuickJSUseAfterFree("Lifetime not alive");
      }
    };
    StaticLifetime = class extends Lifetime {
      constructor(value, owner) {
        super(value, void 0, void 0, owner);
      }
      get dupable() {
        return true;
      }
      dup() {
        return this;
      }
      dispose() {
      }
    };
    WeakLifetime = class extends Lifetime {
      constructor(value, copier, disposer, owner) {
        super(value, copier, disposer, owner);
      }
      dispose() {
        this._alive = false;
      }
    };
    Scope = class _Scope extends UsingDisposable {
      constructor() {
        super(...arguments);
        this._disposables = new Lifetime(/* @__PURE__ */ new Set());
        this.manage = (lifetime) => (this._disposables.value.add(lifetime), lifetime);
      }
      static withScope(block) {
        let scope = new _Scope(), blockError;
        try {
          return block(scope);
        } catch (error) {
          throw blockError = error, error;
        } finally {
          scopeFinally(scope, blockError);
        }
      }
      static withScopeMaybeAsync(_this, block) {
        return maybeAsync(void 0, function* (awaited) {
          let scope = new _Scope(), blockError;
          try {
            return yield* awaited.of(block.call(_this, awaited, scope));
          } catch (error) {
            throw blockError = error, error;
          } finally {
            scopeFinally(scope, blockError);
          }
        });
      }
      static async withScopeAsync(block) {
        let scope = new _Scope(), blockError;
        try {
          return await block(scope);
        } catch (error) {
          throw blockError = error, error;
        } finally {
          scopeFinally(scope, blockError);
        }
      }
      get alive() {
        return this._disposables.alive;
      }
      dispose() {
        let lifetimes = Array.from(this._disposables.value.values()).reverse();
        for (let lifetime of lifetimes) lifetime.alive && lifetime.dispose();
        this._disposables.dispose();
      }
    };
    AbstractDisposableResult = class _AbstractDisposableResult extends UsingDisposable {
      static success(value) {
        return new DisposableSuccess(value);
      }
      static fail(error, onUnwrap) {
        return new DisposableFail(error, onUnwrap);
      }
      static is(result) {
        return result instanceof _AbstractDisposableResult;
      }
    };
    DisposableSuccess = class extends AbstractDisposableResult {
      constructor(value) {
        super();
        this.value = value;
      }
      get alive() {
        return isDisposable(this.value) ? this.value.alive : true;
      }
      dispose() {
        isDisposable(this.value) && this.value.dispose();
      }
      unwrap() {
        return this.value;
      }
      unwrapOr(_fallback) {
        return this.value;
      }
    };
    DisposableFail = class extends AbstractDisposableResult {
      constructor(error, onUnwrap) {
        super();
        this.error = error;
        this.onUnwrap = onUnwrap;
      }
      get alive() {
        return isDisposable(this.error) ? this.error.alive : true;
      }
      dispose() {
        isDisposable(this.error) && this.error.dispose();
      }
      unwrap() {
        throw this.onUnwrap(this), this.error;
      }
      unwrapOr(fallback) {
        return fallback;
      }
    };
    DisposableResult = AbstractDisposableResult;
    QuickJSDeferredPromise = class extends UsingDisposable {
      constructor(args) {
        super();
        this.resolve = (value) => {
          this.resolveHandle.alive && (this.context.unwrapResult(this.context.callFunction(this.resolveHandle, this.context.undefined, value || this.context.undefined)).dispose(), this.disposeResolvers(), this.onSettled());
        };
        this.reject = (value) => {
          this.rejectHandle.alive && (this.context.unwrapResult(this.context.callFunction(this.rejectHandle, this.context.undefined, value || this.context.undefined)).dispose(), this.disposeResolvers(), this.onSettled());
        };
        this.dispose = () => {
          this.handle.alive && this.handle.dispose(), this.disposeResolvers();
        };
        this.context = args.context, this.owner = args.context.runtime, this.handle = args.promiseHandle, this.settled = new Promise((resolve) => {
          this.onSettled = resolve;
        }), this.resolveHandle = args.resolveHandle, this.rejectHandle = args.rejectHandle;
      }
      get alive() {
        return this.handle.alive || this.resolveHandle.alive || this.rejectHandle.alive;
      }
      disposeResolvers() {
        this.resolveHandle.alive && this.resolveHandle.dispose(), this.rejectHandle.alive && this.rejectHandle.dispose();
      }
    };
    ModuleMemory = class {
      constructor(module) {
        this.module = module;
      }
      toPointerArray(handleArray) {
        let typedArray = new Int32Array(handleArray.map((handle) => handle.value)), numBytes = typedArray.length * typedArray.BYTES_PER_ELEMENT, ptr = this.module._malloc(numBytes);
        return new Uint8Array(this.module.HEAPU8.buffer, ptr, numBytes).set(new Uint8Array(typedArray.buffer)), new Lifetime(ptr, void 0, (ptr2) => this.module._free(ptr2));
      }
      newTypedArray(kind, length) {
        let module = this.module, numBytes = length * kind.BYTES_PER_ELEMENT, ptr = module._malloc(numBytes);
        new kind(module.HEAPU8.buffer, ptr, length).fill(0);
        return new Lifetime({ get typedArray() {
          return new kind(module.HEAPU8.buffer, ptr, length);
        }, ptr }, void 0, (value) => module._free(value.ptr));
      }
      newMutablePointerArray(length) {
        return this.newTypedArray(Int32Array, length);
      }
      newHeapCharPointer(string) {
        let strlen = this.module.lengthBytesUTF8(string), dataBytes = strlen + 1, ptr = this.module._malloc(dataBytes);
        return this.module.stringToUTF8(string, ptr, dataBytes), new Lifetime({ ptr, strlen }, void 0, (value) => this.module._free(value.ptr));
      }
      newHeapBufferPointer(buffer) {
        let numBytes = buffer.byteLength, ptr = this.module._malloc(numBytes);
        return this.module.HEAPU8.set(buffer, ptr), new Lifetime({ pointer: ptr, numBytes }, void 0, (value) => this.module._free(value.pointer));
      }
      consumeHeapCharPointer(ptr) {
        let str = this.module.UTF8ToString(ptr);
        return this.module._free(ptr), str;
      }
    };
    DefaultIntrinsics = Object.freeze({ BaseObjects: true, Date: true, Eval: true, StringNormalize: true, RegExp: true, JSON: true, Proxy: true, MapSet: true, TypedArrays: true, Promise: true });
    QuickJSIterator = class extends UsingDisposable {
      constructor(handle, context) {
        super();
        this.handle = handle;
        this.context = context;
        this._isDone = false;
        this.owner = context.runtime;
      }
      [Symbol.iterator]() {
        return this;
      }
      next(value) {
        if (!this.alive || this._isDone) return { done: true, value: void 0 };
        let nextMethod = this._next ?? (this._next = this.context.getProp(this.handle, "next"));
        return this.callIteratorMethod(nextMethod, value);
      }
      return(value) {
        if (!this.alive) return { done: true, value: void 0 };
        let returnMethod = this.context.getProp(this.handle, "return");
        if (returnMethod === this.context.undefined && value === void 0) return this.dispose(), { done: true, value: void 0 };
        let result = this.callIteratorMethod(returnMethod, value);
        return returnMethod.dispose(), this.dispose(), result;
      }
      throw(e) {
        if (!this.alive) return { done: true, value: void 0 };
        let errorHandle = e instanceof Lifetime ? e : this.context.newError(e), throwMethod = this.context.getProp(this.handle, "throw"), result = this.callIteratorMethod(throwMethod, e);
        return errorHandle.alive && errorHandle.dispose(), throwMethod.dispose(), this.dispose(), result;
      }
      get alive() {
        return this.handle.alive;
      }
      dispose() {
        this._isDone = true, this.handle.dispose(), this._next?.dispose();
      }
      callIteratorMethod(method, input) {
        let callResult = input ? this.context.callFunction(method, this.handle, input) : this.context.callFunction(method, this.handle);
        if (callResult.error) return this.dispose(), { value: callResult };
        let done = this.context.getProp(callResult.value, "done").consume((v) => this.context.dump(v)), value = this.context.getProp(callResult.value, "value");
        return callResult.value.dispose(), done && this.dispose(), { value: DisposableResult.success(value), done };
      }
    };
    INT32_MIN = -2147483648;
    INT32_MAX = 2147483647;
    INVALID_HOST_REF_ID = 0;
    HostRefMap = class {
      constructor() {
        this.nextId = INT32_MIN;
        this.freelist = [];
        this.groups = /* @__PURE__ */ new Map();
      }
      put(value) {
        let id = this.allocateId(), groupId = getGroupId(id), group = this.groups.get(groupId);
        return group || (group = /* @__PURE__ */ new Map(), this.groups.set(groupId, group)), group.set(id, value), id;
      }
      get(id) {
        if (id === INVALID_HOST_REF_ID) throw new QuickJSHostRefInvalid("no host reference id defined");
        let groupId = getGroupId(id), group = this.groups.get(groupId);
        if (!group) throw new QuickJSHostRefInvalid(`host reference id ${id} is not defined`);
        let value = group.get(id);
        if (!value) throw new QuickJSHostRefInvalid(`host reference id ${id} is not defined`);
        return value;
      }
      delete(id) {
        if (id === INVALID_HOST_REF_ID) throw new QuickJSHostRefInvalid("no host reference id defined");
        let groupId = getGroupId(id), group = this.groups.get(groupId);
        if (!group) throw new QuickJSHostRefInvalid(`host reference id ${id} is not defined`);
        group.delete(id), group.size === 0 && this.groups.delete(groupId), this.freelist.push(id);
      }
      allocateId() {
        if (this.freelist.length > 0) return this.freelist.shift();
        if (this.nextId === INVALID_HOST_REF_ID && this.nextId++, this.nextId > INT32_MAX) throw new QuickJSHostRefRangeExceeded(`HostRefMap: too many host refs created without disposing. Max simultaneous host refs: ${INT32_MAX - INT32_MIN}`);
        return this.nextId++;
      }
    };
    HostRef = class extends UsingDisposable {
      constructor(runtime, handle, id) {
        if (id === INVALID_HOST_REF_ID) throw new QuickJSHostRefInvalid("cannot create HostRef with undefined id");
        super();
        this.runtime = runtime;
        this.handle = handle;
        this.id = id;
      }
      get alive() {
        return this.handle.alive;
      }
      dispose() {
        this.handle.dispose();
      }
      get value() {
        return this.runtime.hostRefs.get(this.id);
      }
    };
    ContextMemory = class extends ModuleMemory {
      constructor(args) {
        super(args.module);
        this.scope = new Scope();
        this.copyJSValue = (ptr) => this.ffi.QTS_DupValuePointer(this.ctx.value, ptr);
        this.freeJSValue = (ptr) => {
          this.ffi.QTS_FreeValuePointer(this.ctx.value, ptr);
        };
        args.ownedLifetimes?.forEach((lifetime) => this.scope.manage(lifetime)), this.owner = args.owner, this.module = args.module, this.ffi = args.ffi, this.rt = args.rt, this.ctx = this.scope.manage(args.ctx);
      }
      get alive() {
        return this.scope.alive;
      }
      dispose() {
        return this.scope.dispose();
      }
      [Symbol.dispose]() {
        return this.dispose();
      }
      manage(lifetime) {
        return this.scope.manage(lifetime);
      }
      consumeJSCharPointer(ptr) {
        let str = this.module.UTF8ToString(ptr);
        return this.ffi.QTS_FreeCString(this.ctx.value, ptr), str;
      }
      heapValueHandle(ptr, extraDispose) {
        let dispose = extraDispose ? (val) => {
          extraDispose(), this.freeJSValue(val);
        } : this.freeJSValue;
        return new Lifetime(ptr, this.copyJSValue, dispose, this.owner);
      }
      staticHeapValueHandle(ptr) {
        return this.manage(this.heapValueHandle(ptr)), new StaticLifetime(ptr, this.owner);
      }
    };
    QuickJSContext = class extends UsingDisposable {
      constructor(args) {
        super();
        this._undefined = void 0;
        this._null = void 0;
        this._false = void 0;
        this._true = void 0;
        this._global = void 0;
        this._BigInt = void 0;
        this._Symbol = void 0;
        this._SymbolIterator = void 0;
        this._SymbolAsyncIterator = void 0;
        this.cToHostCallbacks = { callFunction: (ctx, this_ptr, argc, argv, fn_id) => {
          if (ctx !== this.ctx.value) throw new Error("QuickJSContext instance received C -> JS call with mismatched ctx");
          let fn = this.getFunction(fn_id);
          return Scope.withScopeMaybeAsync(this, function* (awaited, scope) {
            let thisHandle = scope.manage(new WeakLifetime(this_ptr, this.memory.copyJSValue, this.memory.freeJSValue, this.runtime)), argHandles = new Array(argc);
            for (let i = 0; i < argc; i++) {
              let ptr = this.ffi.QTS_ArgvGetJSValueConstPointer(argv, i);
              argHandles[i] = scope.manage(new WeakLifetime(ptr, this.memory.copyJSValue, this.memory.freeJSValue, this.runtime));
            }
            try {
              let result = yield* awaited(fn.apply(thisHandle, argHandles));
              if (result) {
                if ("error" in result && result.error) throw this.runtime.debugLog("throw error", result.error), result.error;
                let handle = scope.manage(result instanceof Lifetime ? result : result.value);
                return this.ffi.QTS_DupValuePointer(this.ctx.value, handle.value);
              }
              return 0;
            } catch (error) {
              return this.errorToHandle(error).consume((errorHandle) => this.ffi.QTS_Throw(this.ctx.value, errorHandle.value));
            }
          });
        } };
        this.runtime = args.runtime, this.module = args.module, this.ffi = args.ffi, this.rt = args.rt, this.ctx = args.ctx, this.memory = new ContextMemory({ ...args, owner: this.runtime }), args.callbacks.setContextCallbacks(this.ctx.value, this.cToHostCallbacks), this.dump = this.dump.bind(this), this.getString = this.getString.bind(this), this.getNumber = this.getNumber.bind(this), this.resolvePromise = this.resolvePromise.bind(this), this.uint32Out = this.memory.manage(this.memory.newTypedArray(Uint32Array, 1));
      }
      get alive() {
        return this.memory.alive;
      }
      dispose() {
        this.memory.dispose();
      }
      get undefined() {
        if (this._undefined) return this._undefined;
        let ptr = this.ffi.QTS_GetUndefined();
        return this._undefined = new StaticLifetime(ptr);
      }
      get null() {
        if (this._null) return this._null;
        let ptr = this.ffi.QTS_GetNull();
        return this._null = new StaticLifetime(ptr);
      }
      get true() {
        if (this._true) return this._true;
        let ptr = this.ffi.QTS_GetTrue();
        return this._true = new StaticLifetime(ptr);
      }
      get false() {
        if (this._false) return this._false;
        let ptr = this.ffi.QTS_GetFalse();
        return this._false = new StaticLifetime(ptr);
      }
      get global() {
        if (this._global) return this._global;
        let ptr = this.ffi.QTS_GetGlobalObject(this.ctx.value);
        return this._global = this.memory.staticHeapValueHandle(ptr), this._global;
      }
      newNumber(num) {
        return this.memory.heapValueHandle(this.ffi.QTS_NewFloat64(this.ctx.value, num));
      }
      newString(str) {
        let ptr = this.memory.newHeapCharPointer(str).consume((charHandle) => this.ffi.QTS_NewString(this.ctx.value, charHandle.value.ptr));
        return this.memory.heapValueHandle(ptr);
      }
      newUniqueSymbol(description) {
        let key = (typeof description == "symbol" ? description.description : description) ?? "", ptr = this.memory.newHeapCharPointer(key).consume((charHandle) => this.ffi.QTS_NewSymbol(this.ctx.value, charHandle.value.ptr, 0));
        return this.memory.heapValueHandle(ptr);
      }
      newSymbolFor(key) {
        let description = (typeof key == "symbol" ? key.description : key) ?? "", ptr = this.memory.newHeapCharPointer(description).consume((charHandle) => this.ffi.QTS_NewSymbol(this.ctx.value, charHandle.value.ptr, 1));
        return this.memory.heapValueHandle(ptr);
      }
      getWellKnownSymbol(name) {
        return this._Symbol ?? (this._Symbol = this.memory.manage(this.getProp(this.global, "Symbol"))), this.getProp(this._Symbol, name);
      }
      newBigInt(num) {
        if (!this._BigInt) {
          let bigIntHandle2 = this.getProp(this.global, "BigInt");
          this.memory.manage(bigIntHandle2), this._BigInt = new StaticLifetime(bigIntHandle2.value, this.runtime);
        }
        let bigIntHandle = this._BigInt, asString = String(num);
        return this.newString(asString).consume((handle) => this.unwrapResult(this.callFunction(bigIntHandle, this.undefined, handle)));
      }
      newObject(prototype) {
        prototype && this.runtime.assertOwned(prototype);
        let ptr = prototype ? this.ffi.QTS_NewObjectProto(this.ctx.value, prototype.value) : this.ffi.QTS_NewObject(this.ctx.value);
        return this.memory.heapValueHandle(ptr);
      }
      newArray() {
        let ptr = this.ffi.QTS_NewArray(this.ctx.value);
        return this.memory.heapValueHandle(ptr);
      }
      newArrayBuffer(buffer) {
        let array = new Uint8Array(buffer), handle = this.memory.newHeapBufferPointer(array), ptr = this.ffi.QTS_NewArrayBuffer(this.ctx.value, handle.value.pointer, array.length);
        return this.memory.heapValueHandle(ptr);
      }
      newPromise(value) {
        let deferredPromise = Scope.withScope((scope) => {
          let mutablePointerArray = scope.manage(this.memory.newMutablePointerArray(2)), promisePtr = this.ffi.QTS_NewPromiseCapability(this.ctx.value, mutablePointerArray.value.ptr), promiseHandle = this.memory.heapValueHandle(promisePtr), [resolveHandle, rejectHandle] = Array.from(mutablePointerArray.value.typedArray).map((jsvaluePtr) => this.memory.heapValueHandle(jsvaluePtr));
          return new QuickJSDeferredPromise({ context: this, promiseHandle, resolveHandle, rejectHandle });
        });
        return value && typeof value == "function" && (value = new Promise(value)), value && Promise.resolve(value).then(deferredPromise.resolve, (error) => error instanceof Lifetime ? deferredPromise.reject(error) : this.newError(error).consume(deferredPromise.reject)), deferredPromise;
      }
      newFunction(nameOrFn, maybeFn) {
        let fn = typeof nameOrFn == "function" ? nameOrFn : maybeFn;
        if (!fn) throw new TypeError("Expected a function");
        return this.newFunctionWithOptions({ name: typeof nameOrFn == "string" ? nameOrFn : void 0, length: fn.length, isConstructor: false, fn });
      }
      newConstructorFunction(nameOrFn, maybeFn) {
        let fn = typeof nameOrFn == "function" ? nameOrFn : maybeFn;
        if (!fn) throw new TypeError("Expected a function");
        return this.newFunctionWithOptions({ name: typeof nameOrFn == "string" ? nameOrFn : void 0, length: fn.length, isConstructor: true, fn });
      }
      newFunctionWithOptions(args) {
        let { name, length, isConstructor, fn } = args, refId = this.runtime.hostRefs.put(fn);
        try {
          return this.memory.heapValueHandle(this.ffi.QTS_NewFunction(this.ctx.value, name ?? "", length, isConstructor, refId));
        } catch (error) {
          throw this.runtime.hostRefs.delete(refId), error;
        }
      }
      newError(error) {
        let errorHandle = this.memory.heapValueHandle(this.ffi.QTS_NewError(this.ctx.value));
        return error && typeof error == "object" ? (error.name !== void 0 && this.newString(error.name).consume((handle) => this.setProp(errorHandle, "name", handle)), error.message !== void 0 && this.newString(error.message).consume((handle) => this.setProp(errorHandle, "message", handle))) : typeof error == "string" ? this.newString(error).consume((handle) => this.setProp(errorHandle, "message", handle)) : error !== void 0 && this.newString(String(error)).consume((handle) => this.setProp(errorHandle, "message", handle)), errorHandle;
      }
      newHostRef(value) {
        let id = this.runtime.hostRefs.put(value);
        try {
          let handle = this.memory.heapValueHandle(this.ffi.QTS_NewHostRef(this.ctx.value, id));
          return new HostRef(this.runtime, handle, id);
        } catch (error) {
          throw this.runtime.hostRefs.delete(id), error;
        }
      }
      toHostRef(handle) {
        let id = this.ffi.QTS_GetHostRefId(handle.value);
        if (id !== 0) return this.runtime.hostRefs.get(id), new HostRef(this.runtime, handle.dup(), id);
      }
      unwrapHostRef(handle) {
        let id = this.ffi.QTS_GetHostRefId(handle.value);
        if (id === 0) throw new QuickJSHostRefInvalid("handle is not a HostRef");
        return this.runtime.hostRefs.get(id);
      }
      typeof(handle) {
        return this.runtime.assertOwned(handle), this.memory.consumeHeapCharPointer(this.ffi.QTS_Typeof(this.ctx.value, handle.value));
      }
      getNumber(handle) {
        return this.runtime.assertOwned(handle), this.ffi.QTS_GetFloat64(this.ctx.value, handle.value);
      }
      getString(handle) {
        return this.runtime.assertOwned(handle), this.memory.consumeJSCharPointer(this.ffi.QTS_GetString(this.ctx.value, handle.value));
      }
      getSymbol(handle) {
        this.runtime.assertOwned(handle);
        let key = this.memory.consumeJSCharPointer(this.ffi.QTS_GetSymbolDescriptionOrKey(this.ctx.value, handle.value));
        return this.ffi.QTS_IsGlobalSymbol(this.ctx.value, handle.value) ? Symbol.for(key) : Symbol(key);
      }
      getBigInt(handle) {
        this.runtime.assertOwned(handle);
        let asString = this.getString(handle);
        return BigInt(asString);
      }
      getArrayBuffer(handle) {
        this.runtime.assertOwned(handle);
        let len = this.ffi.QTS_GetArrayBufferLength(this.ctx.value, handle.value), ptr = this.ffi.QTS_GetArrayBuffer(this.ctx.value, handle.value);
        if (!ptr) throw new Error("Couldn't allocate memory to get ArrayBuffer");
        return new Lifetime(this.module.HEAPU8.subarray(ptr, ptr + len), void 0, () => this.module._free(ptr));
      }
      getPromiseState(handle) {
        this.runtime.assertOwned(handle);
        let state = this.ffi.QTS_PromiseState(this.ctx.value, handle.value);
        if (state < 0) return { type: "fulfilled", value: handle, notAPromise: true };
        if (state === JSPromiseStateEnum.Pending) return { type: "pending", get error() {
          return new QuickJSPromisePending("Cannot unwrap a pending promise");
        } };
        let ptr = this.ffi.QTS_PromiseResult(this.ctx.value, handle.value), result = this.memory.heapValueHandle(ptr);
        if (state === JSPromiseStateEnum.Fulfilled) return { type: "fulfilled", value: result };
        if (state === JSPromiseStateEnum.Rejected) return { type: "rejected", error: result };
        throw result.dispose(), new Error(`Unknown JSPromiseStateEnum: ${state}`);
      }
      resolvePromise(promiseLikeHandle) {
        this.runtime.assertOwned(promiseLikeHandle);
        let vmResolveResult = Scope.withScope((scope) => {
          let vmPromise = scope.manage(this.getProp(this.global, "Promise")), vmPromiseResolve = scope.manage(this.getProp(vmPromise, "resolve"));
          return this.callFunction(vmPromiseResolve, vmPromise, promiseLikeHandle);
        });
        return vmResolveResult.error ? Promise.resolve(vmResolveResult) : new Promise((resolve) => {
          Scope.withScope((scope) => {
            let resolveHandle = scope.manage(this.newFunction("resolve", (value) => {
              resolve(this.success(value && value.dup()));
            })), rejectHandle = scope.manage(this.newFunction("reject", (error) => {
              resolve(this.fail(error && error.dup()));
            })), promiseHandle = scope.manage(vmResolveResult.value), promiseThenHandle = scope.manage(this.getProp(promiseHandle, "then"));
            this.callFunction(promiseThenHandle, promiseHandle, resolveHandle, rejectHandle).unwrap().dispose();
          });
        });
      }
      isEqual(a, b, equalityType = IsEqualOp.IsStrictlyEqual) {
        if (a === b) return true;
        this.runtime.assertOwned(a), this.runtime.assertOwned(b);
        let result = this.ffi.QTS_IsEqual(this.ctx.value, a.value, b.value, equalityType);
        if (result === -1) throw new QuickJSNotImplemented("WASM variant does not expose equality");
        return !!result;
      }
      eq(handle, other) {
        return this.isEqual(handle, other, IsEqualOp.IsStrictlyEqual);
      }
      sameValue(handle, other) {
        return this.isEqual(handle, other, IsEqualOp.IsSameValue);
      }
      sameValueZero(handle, other) {
        return this.isEqual(handle, other, IsEqualOp.IsSameValueZero);
      }
      getProp(handle, key) {
        this.runtime.assertOwned(handle);
        let ptr;
        return typeof key == "number" && key >= 0 ? ptr = this.ffi.QTS_GetPropNumber(this.ctx.value, handle.value, key) : ptr = this.borrowPropertyKey(key).consume((quickJSKey) => this.ffi.QTS_GetProp(this.ctx.value, handle.value, quickJSKey.value)), this.memory.heapValueHandle(ptr);
      }
      getLength(handle) {
        if (this.runtime.assertOwned(handle), !(this.ffi.QTS_GetLength(this.ctx.value, this.uint32Out.value.ptr, handle.value) < 0)) return this.uint32Out.value.typedArray[0];
      }
      getOwnPropertyNames(handle, options = { strings: true, numbersAsStrings: true }) {
        this.runtime.assertOwned(handle), handle.value;
        let flags = getOwnPropertyNamesOptionsToFlags(options);
        if (flags === 0) throw new QuickJSEmptyGetOwnPropertyNames("No options set, will return an empty array");
        return Scope.withScope((scope) => {
          let outPtr = scope.manage(this.memory.newMutablePointerArray(1)), errorPtr = this.ffi.QTS_GetOwnPropertyNames(this.ctx.value, outPtr.value.ptr, this.uint32Out.value.ptr, handle.value, flags);
          if (errorPtr) return this.fail(this.memory.heapValueHandle(errorPtr));
          let len = this.uint32Out.value.typedArray[0], ptr = outPtr.value.typedArray[0], pointerArray = new Uint32Array(this.module.HEAP8.buffer, ptr, len), handles = Array.from(pointerArray).map((ptr2) => this.memory.heapValueHandle(ptr2));
          return this.ffi.QTS_FreeVoidPointer(this.ctx.value, ptr), this.success(createDisposableArray(handles));
        });
      }
      getIterator(iterableHandle) {
        let SymbolIterator = this._SymbolIterator ?? (this._SymbolIterator = this.memory.manage(this.getWellKnownSymbol("iterator")));
        return Scope.withScope((scope) => {
          let methodHandle = scope.manage(this.getProp(iterableHandle, SymbolIterator)), iteratorCallResult = this.callFunction(methodHandle, iterableHandle);
          return iteratorCallResult.error ? iteratorCallResult : this.success(new QuickJSIterator(iteratorCallResult.value, this));
        });
      }
      setProp(handle, key, value) {
        this.runtime.assertOwned(handle), this.borrowPropertyKey(key).consume((quickJSKey) => this.ffi.QTS_SetProp(this.ctx.value, handle.value, quickJSKey.value, value.value));
      }
      defineProp(handle, key, descriptor) {
        this.runtime.assertOwned(handle), Scope.withScope((scope) => {
          let quickJSKey = scope.manage(this.borrowPropertyKey(key)), value = descriptor.value || this.undefined, configurable = !!descriptor.configurable, enumerable = !!descriptor.enumerable, hasValue = !!descriptor.value, get = descriptor.get ? scope.manage(this.newFunction(descriptor.get.name, descriptor.get)) : this.undefined, set = descriptor.set ? scope.manage(this.newFunction(descriptor.set.name, descriptor.set)) : this.undefined;
          this.ffi.QTS_DefineProp(this.ctx.value, handle.value, quickJSKey.value, value.value, get.value, set.value, configurable, enumerable, hasValue);
        });
      }
      callFunction(func, thisVal, ...restArgs) {
        this.runtime.assertOwned(func);
        let args, firstArg = restArgs[0];
        firstArg === void 0 || Array.isArray(firstArg) ? args = firstArg ?? [] : args = restArgs;
        let resultPtr = this.memory.toPointerArray(args).consume((argsArrayPtr) => this.ffi.QTS_Call(this.ctx.value, func.value, thisVal.value, args.length, argsArrayPtr.value)), errorPtr = this.ffi.QTS_ResolveException(this.ctx.value, resultPtr);
        return errorPtr ? (this.ffi.QTS_FreeValuePointer(this.ctx.value, resultPtr), this.fail(this.memory.heapValueHandle(errorPtr))) : this.success(this.memory.heapValueHandle(resultPtr));
      }
      callMethod(thisHandle, key, args = []) {
        return this.getProp(thisHandle, key).consume((func) => this.callFunction(func, thisHandle, args));
      }
      evalCode(code, filename = "eval.js", options) {
        let detectModule = options === void 0 ? 1 : 0, flags = evalOptionsToFlags(options), resultPtr = this.memory.newHeapCharPointer(code).consume((charHandle) => this.ffi.QTS_Eval(this.ctx.value, charHandle.value.ptr, charHandle.value.strlen, filename, detectModule, flags)), errorPtr = this.ffi.QTS_ResolveException(this.ctx.value, resultPtr);
        return errorPtr ? (this.ffi.QTS_FreeValuePointer(this.ctx.value, resultPtr), this.fail(this.memory.heapValueHandle(errorPtr))) : this.success(this.memory.heapValueHandle(resultPtr));
      }
      throw(error) {
        return this.errorToHandle(error).consume((handle) => this.ffi.QTS_Throw(this.ctx.value, handle.value));
      }
      borrowPropertyKey(key) {
        return typeof key == "number" ? this.newNumber(key) : typeof key == "string" ? this.newString(key) : new StaticLifetime(key.value, this.runtime);
      }
      getMemory(rt) {
        if (rt === this.rt.value) return this.memory;
        throw new Error("Private API. Cannot get memory from a different runtime");
      }
      dump(handle) {
        this.runtime.assertOwned(handle);
        let type = this.typeof(handle);
        if (type === "string") return this.getString(handle);
        if (type === "number") return this.getNumber(handle);
        if (type === "bigint") return this.getBigInt(handle);
        if (type === "undefined") return;
        if (type === "symbol") return this.getSymbol(handle);
        let asPromiseState = this.getPromiseState(handle);
        if (asPromiseState.type === "fulfilled" && !asPromiseState.notAPromise) return handle.dispose(), { type: asPromiseState.type, value: asPromiseState.value.consume(this.dump) };
        if (asPromiseState.type === "pending") return handle.dispose(), { type: asPromiseState.type };
        if (asPromiseState.type === "rejected") return handle.dispose(), { type: asPromiseState.type, error: asPromiseState.error.consume(this.dump) };
        let str = this.memory.consumeJSCharPointer(this.ffi.QTS_Dump(this.ctx.value, handle.value));
        try {
          return JSON.parse(str);
        } catch {
          return str;
        }
      }
      unwrapResult(result) {
        if (result.error) {
          let context = "context" in result.error ? result.error.context : this, cause = result.error.consume((error) => this.dump(error));
          if (cause && typeof cause == "object" && typeof cause.message == "string") {
            let { message, name, stack, ...rest } = cause, exception = new QuickJSUnwrapError(cause, context);
            typeof name == "string" && (exception.name = cause.name), exception.message = message;
            let hostStack = exception.stack;
            throw typeof stack == "string" && (exception.stack = `${name}: ${message}
${cause.stack}Host: ${hostStack}`), Object.assign(exception, rest), exception;
          }
          throw new QuickJSUnwrapError(cause);
        }
        return result.value;
      }
      [/* @__PURE__ */ Symbol.for("nodejs.util.inspect.custom")]() {
        return this.alive ? `${this.constructor.name} { ctx: ${this.ctx.value} rt: ${this.rt.value} }` : `${this.constructor.name} { disposed }`;
      }
      getFunction(fn_id) {
        let fn = this.runtime.hostRefs.get(fn_id);
        if (typeof fn != "function") throw new Error(`Host reference ${fn_id} is not a function`);
        return fn;
      }
      errorToHandle(error) {
        return error instanceof Lifetime ? error : this.newError(error);
      }
      encodeBinaryJSON(handle) {
        let ptr = this.ffi.QTS_bjson_encode(this.ctx.value, handle.value);
        return this.memory.heapValueHandle(ptr);
      }
      decodeBinaryJSON(handle) {
        let ptr = this.ffi.QTS_bjson_decode(this.ctx.value, handle.value);
        return this.memory.heapValueHandle(ptr);
      }
      success(value) {
        return DisposableResult.success(value);
      }
      fail(error) {
        return DisposableResult.fail(error, (error2) => this.unwrapResult(error2));
      }
    };
    QuickJSRuntime = class extends UsingDisposable {
      constructor(args) {
        super();
        this.scope = new Scope();
        this.contextMap = /* @__PURE__ */ new Map();
        this.hostRefs = new HostRefMap();
        this._debugMode = false;
        this.cToHostCallbacks = { freeHostRef: (rt, host_ref_id) => {
          if (rt !== this.rt.value) throw new Error("Runtime pointer mismatch");
          this.hostRefs.delete(host_ref_id);
        }, shouldInterrupt: (rt) => {
          if (rt !== this.rt.value) throw new Error("QuickJSContext instance received C -> JS interrupt with mismatched rt");
          let fn = this.interruptHandler;
          if (!fn) throw new Error("QuickJSContext had no interrupt handler");
          return fn(this) ? 1 : 0;
        }, loadModuleSource: maybeAsyncFn(this, function* (awaited, rt, ctx, moduleName) {
          let moduleLoader = this.moduleLoader;
          if (!moduleLoader) throw new Error("Runtime has no module loader");
          if (rt !== this.rt.value) throw new Error("Runtime pointer mismatch");
          let context = this.contextMap.get(ctx) ?? this.newContext({ contextPointer: ctx });
          try {
            let result = yield* awaited(moduleLoader(moduleName, context));
            if (typeof result == "object" && "error" in result && result.error) throw this.debugLog("cToHostLoadModule: loader returned error", result.error), result.error;
            let moduleSource = typeof result == "string" ? result : "value" in result ? result.value : result;
            return this.memory.newHeapCharPointer(moduleSource).value.ptr;
          } catch (error) {
            return this.debugLog("cToHostLoadModule: caught error", error), context.throw(error), 0;
          }
        }), normalizeModule: maybeAsyncFn(this, function* (awaited, rt, ctx, baseModuleName, moduleNameRequest) {
          let moduleNormalizer = this.moduleNormalizer;
          if (!moduleNormalizer) throw new Error("Runtime has no module normalizer");
          if (rt !== this.rt.value) throw new Error("Runtime pointer mismatch");
          let context = this.contextMap.get(ctx) ?? this.newContext({ contextPointer: ctx });
          try {
            let result = yield* awaited(moduleNormalizer(baseModuleName, moduleNameRequest, context));
            if (typeof result == "object" && "error" in result && result.error) throw this.debugLog("cToHostNormalizeModule: normalizer returned error", result.error), result.error;
            let name = typeof result == "string" ? result : result.value;
            return context.getMemory(this.rt.value).newHeapCharPointer(name).value.ptr;
          } catch (error) {
            return this.debugLog("normalizeModule: caught error", error), context.throw(error), 0;
          }
        }) };
        args.ownedLifetimes?.forEach((lifetime) => this.scope.manage(lifetime)), this.module = args.module, this.memory = new ModuleMemory(this.module), this.ffi = args.ffi, this.rt = args.rt, this.callbacks = args.callbacks, this.scope.manage(this.rt), this.callbacks.setRuntimeCallbacks(this.rt.value, this.cToHostCallbacks), this.executePendingJobs = this.executePendingJobs.bind(this), QTS_DEBUG && this.setDebugMode(true);
      }
      get alive() {
        return this.scope.alive;
      }
      dispose() {
        return this.scope.dispose();
      }
      newContext(options = {}) {
        let intrinsics = intrinsicsToFlags(options.intrinsics), ctx = new Lifetime(options.contextPointer || this.ffi.QTS_NewContext(this.rt.value, intrinsics), void 0, (ctx_ptr) => {
          this.contextMap.delete(ctx_ptr), this.callbacks.deleteContext(ctx_ptr), this.ffi.QTS_FreeContext(ctx_ptr);
        }), context = new QuickJSContext({ module: this.module, ctx, ffi: this.ffi, rt: this.rt, ownedLifetimes: options.ownedLifetimes, runtime: this, callbacks: this.callbacks });
        return this.contextMap.set(ctx.value, context), context;
      }
      setModuleLoader(moduleLoader, moduleNormalizer) {
        this.moduleLoader = moduleLoader, this.moduleNormalizer = moduleNormalizer, this.ffi.QTS_RuntimeEnableModuleLoader(this.rt.value, this.moduleNormalizer ? 1 : 0);
      }
      removeModuleLoader() {
        this.moduleLoader = void 0, this.ffi.QTS_RuntimeDisableModuleLoader(this.rt.value);
      }
      hasPendingJob() {
        return !!this.ffi.QTS_IsJobPending(this.rt.value);
      }
      setInterruptHandler(cb) {
        let prevInterruptHandler = this.interruptHandler;
        this.interruptHandler = cb, prevInterruptHandler || this.ffi.QTS_RuntimeEnableInterruptHandler(this.rt.value);
      }
      removeInterruptHandler() {
        this.interruptHandler && (this.ffi.QTS_RuntimeDisableInterruptHandler(this.rt.value), this.interruptHandler = void 0);
      }
      executePendingJobs(maxJobsToExecute = -1) {
        let ctxPtrOut = this.memory.newMutablePointerArray(1), valuePtr = this.ffi.QTS_ExecutePendingJob(this.rt.value, maxJobsToExecute ?? -1, ctxPtrOut.value.ptr), ctxPtr = ctxPtrOut.value.typedArray[0];
        if (ctxPtrOut.dispose(), ctxPtr === 0) return this.ffi.QTS_FreeValuePointerRuntime(this.rt.value, valuePtr), DisposableResult.success(0);
        let context = this.contextMap.get(ctxPtr) ?? this.newContext({ contextPointer: ctxPtr }), resultValue = context.getMemory(this.rt.value).heapValueHandle(valuePtr);
        if (context.typeof(resultValue) === "number") {
          let executedJobs = context.getNumber(resultValue);
          return resultValue.dispose(), DisposableResult.success(executedJobs);
        } else {
          let error = Object.assign(resultValue, { context });
          return DisposableResult.fail(error, (error2) => context.unwrapResult(error2));
        }
      }
      setMemoryLimit(limitBytes) {
        if (limitBytes < 0 && limitBytes !== -1) throw new Error("Cannot set memory limit to negative number. To unset, pass -1");
        this.ffi.QTS_RuntimeSetMemoryLimit(this.rt.value, limitBytes);
      }
      computeMemoryUsage() {
        let serviceContextMemory = this.getSystemContext().getMemory(this.rt.value);
        return serviceContextMemory.heapValueHandle(this.ffi.QTS_RuntimeComputeMemoryUsage(this.rt.value, serviceContextMemory.ctx.value));
      }
      dumpMemoryUsage() {
        return this.memory.consumeHeapCharPointer(this.ffi.QTS_RuntimeDumpMemoryUsage(this.rt.value));
      }
      setMaxStackSize(stackSize) {
        if (stackSize < 0) throw new Error("Cannot set memory limit to negative number. To unset, pass 0.");
        this.ffi.QTS_RuntimeSetMaxStackSize(this.rt.value, stackSize);
      }
      assertOwned(handle) {
        if (handle.owner && handle.owner.rt !== this.rt) throw new QuickJSWrongOwner(`Handle is not owned by this runtime: ${handle.owner.rt.value} != ${this.rt.value}`);
      }
      setDebugMode(enabled) {
        this._debugMode = enabled, this.ffi.DEBUG && this.rt.alive && this.ffi.QTS_SetDebugLogEnabled(this.rt.value, enabled ? 1 : 0);
      }
      isDebugMode() {
        return this._debugMode;
      }
      debugLog(...msg) {
        this._debugMode && console.log("quickjs-emscripten:", ...msg);
      }
      [/* @__PURE__ */ Symbol.for("nodejs.util.inspect.custom")]() {
        return this.alive ? `${this.constructor.name} { rt: ${this.rt.value} }` : `${this.constructor.name} { disposed }`;
      }
      getSystemContext() {
        return this.context || (this.context = this.scope.manage(this.newContext())), this.context;
      }
    };
    QuickJSEmscriptenModuleCallbacks = class {
      constructor(args) {
        this.freeHostRef = args.freeHostRef, this.callFunction = args.callFunction, this.shouldInterrupt = args.shouldInterrupt, this.loadModuleSource = args.loadModuleSource, this.normalizeModule = args.normalizeModule;
      }
    };
    QuickJSModuleCallbacks = class {
      constructor(module) {
        this.contextCallbacks = /* @__PURE__ */ new Map();
        this.runtimeCallbacks = /* @__PURE__ */ new Map();
        this.suspendedCount = 0;
        this.cToHostCallbacks = new QuickJSEmscriptenModuleCallbacks({ freeHostRef: (_asyncify, rt, host_ref_id) => {
          let runtimeCallbacks = this.runtimeCallbacks.get(rt);
          if (!runtimeCallbacks) throw new Error(`QuickJSRuntime(rt = ${rt}) not found when trying to free HostRef(id = ${host_ref_id})`);
          runtimeCallbacks.freeHostRef(rt, host_ref_id);
        }, callFunction: (asyncify, ctx, this_ptr, argc, argv, fn_id) => this.handleAsyncify(asyncify, () => {
          try {
            let vm = this.contextCallbacks.get(ctx);
            if (!vm) throw new Error(`QuickJSContext(ctx = ${ctx}) not found for C function call "${fn_id}"`);
            return vm.callFunction(ctx, this_ptr, argc, argv, fn_id);
          } catch (error) {
            return console.error("[C to host error: returning null]", error), 0;
          }
        }), shouldInterrupt: (asyncify, rt) => this.handleAsyncify(asyncify, () => {
          try {
            let vm = this.runtimeCallbacks.get(rt);
            if (!vm) throw new Error(`QuickJSRuntime(rt = ${rt}) not found for C interrupt`);
            return vm.shouldInterrupt(rt);
          } catch (error) {
            return console.error("[C to host interrupt: returning error]", error), 1;
          }
        }), loadModuleSource: (asyncify, rt, ctx, moduleName) => this.handleAsyncify(asyncify, () => {
          try {
            let runtimeCallbacks = this.runtimeCallbacks.get(rt);
            if (!runtimeCallbacks) throw new Error(`QuickJSRuntime(rt = ${rt}) not found for C module loader`);
            let loadModule = runtimeCallbacks.loadModuleSource;
            if (!loadModule) throw new Error(`QuickJSRuntime(rt = ${rt}) does not support module loading`);
            return loadModule(rt, ctx, moduleName);
          } catch (error) {
            return console.error("[C to host module loader error: returning null]", error), 0;
          }
        }), normalizeModule: (asyncify, rt, ctx, moduleBaseName, moduleName) => this.handleAsyncify(asyncify, () => {
          try {
            let runtimeCallbacks = this.runtimeCallbacks.get(rt);
            if (!runtimeCallbacks) throw new Error(`QuickJSRuntime(rt = ${rt}) not found for C module loader`);
            let normalizeModule = runtimeCallbacks.normalizeModule;
            if (!normalizeModule) throw new Error(`QuickJSRuntime(rt = ${rt}) does not support module loading`);
            return normalizeModule(rt, ctx, moduleBaseName, moduleName);
          } catch (error) {
            return console.error("[C to host module loader error: returning null]", error), 0;
          }
        }) });
        this.module = module, this.module.callbacks = this.cToHostCallbacks;
      }
      setRuntimeCallbacks(rt, callbacks) {
        this.runtimeCallbacks.set(rt, callbacks);
      }
      deleteRuntime(rt) {
        this.runtimeCallbacks.delete(rt);
      }
      setContextCallbacks(ctx, callbacks) {
        this.contextCallbacks.set(ctx, callbacks);
      }
      deleteContext(ctx) {
        this.contextCallbacks.delete(ctx);
      }
      handleAsyncify(asyncify, fn) {
        if (asyncify) return asyncify.handleSleep((done) => {
          try {
            let result = fn();
            if (!(result instanceof Promise)) {
              debugLog("asyncify.handleSleep: not suspending:", result), done(result);
              return;
            }
            if (this.suspended) throw new QuickJSAsyncifyError(`Already suspended at: ${this.suspended.stack}
Attempted to suspend at:`);
            this.suspended = new QuickJSAsyncifySuspended(`(${this.suspendedCount++})`), debugLog("asyncify.handleSleep: suspending:", this.suspended), result.then((resolvedResult) => {
              this.suspended = void 0, debugLog("asyncify.handleSleep: resolved:", resolvedResult), done(resolvedResult);
            }, (error) => {
              debugLog("asyncify.handleSleep: rejected:", error), console.error("QuickJS: cannot handle error in suspended function", error), this.suspended = void 0;
            });
          } catch (error) {
            throw debugLog("asyncify.handleSleep: error:", error), this.suspended = void 0, error;
          }
        });
        let value = fn();
        if (value instanceof Promise) throw new Error("Promise return value not supported in non-asyncify context.");
        return value;
      }
    };
    QuickJSWASMModule = class {
      constructor(module, ffi) {
        this.module = module, this.ffi = ffi, this.callbacks = new QuickJSModuleCallbacks(module);
      }
      newRuntime(options = {}) {
        let rt = new Lifetime(this.ffi.QTS_NewRuntime(), void 0, (rt_ptr) => {
          this.ffi.QTS_FreeRuntime(rt_ptr), this.callbacks.deleteRuntime(rt_ptr);
        }), runtime = new QuickJSRuntime({ module: this.module, callbacks: this.callbacks, ffi: this.ffi, rt });
        return applyBaseRuntimeOptions(runtime, options), options.moduleLoader && runtime.setModuleLoader(options.moduleLoader), runtime;
      }
      newContext(options = {}) {
        let runtime = this.newRuntime(), context = runtime.newContext({ ...options, ownedLifetimes: concat(runtime, options.ownedLifetimes) });
        return runtime.context = context, context;
      }
      evalCode(code, options = {}) {
        return Scope.withScope((scope) => {
          let vm = scope.manage(this.newContext());
          applyModuleEvalRuntimeOptions(vm.runtime, options);
          let result = vm.evalCode(code, "eval.js");
          if (options.memoryLimitBytes !== void 0 && vm.runtime.setMemoryLimit(-1), result.error) throw vm.dump(scope.manage(result.error));
          return vm.dump(scope.manage(result.value));
        });
      }
      getWasmMemory() {
        let memory = this.module.quickjsEmscriptenInit?.(() => {
        })?.getWasmMemory?.();
        if (!memory) throw new Error("Variant does not support getting WebAssembly.Memory");
        return memory;
      }
      getFFI() {
        return this.ffi;
      }
    };
  }
});

// node_modules/quickjs-emscripten-core/dist/module-ES6BEMUI.mjs
var module_ES6BEMUI_exports = {};
__export(module_ES6BEMUI_exports, {
  QuickJSModuleCallbacks: () => QuickJSModuleCallbacks,
  QuickJSWASMModule: () => QuickJSWASMModule,
  applyBaseRuntimeOptions: () => applyBaseRuntimeOptions,
  applyModuleEvalRuntimeOptions: () => applyModuleEvalRuntimeOptions
});
var init_module_ES6BEMUI = __esm({
  "node_modules/quickjs-emscripten-core/dist/module-ES6BEMUI.mjs"() {
    init_chunk_V2S4ZYJR();
  }
});

// node_modules/@jitl/quickjs-ng-wasmfile-release-sync/dist/ffi.mjs
var ffi_exports = {};
__export(ffi_exports, {
  QuickJSFFI: () => QuickJSFFI
});
var QuickJSFFI;
var init_ffi = __esm({
  "node_modules/@jitl/quickjs-ng-wasmfile-release-sync/dist/ffi.mjs"() {
    QuickJSFFI = class {
      constructor(module) {
        this.module = module;
        this.DEBUG = false;
        this.QTS_Throw = this.module.cwrap("QTS_Throw", "number", ["number", "number"]);
        this.QTS_NewError = this.module.cwrap("QTS_NewError", "number", ["number"]);
        this.QTS_RuntimeSetMemoryLimit = this.module.cwrap("QTS_RuntimeSetMemoryLimit", null, ["number", "number"]);
        this.QTS_RuntimeComputeMemoryUsage = this.module.cwrap("QTS_RuntimeComputeMemoryUsage", "number", ["number", "number"]);
        this.QTS_RuntimeDumpMemoryUsage = this.module.cwrap("QTS_RuntimeDumpMemoryUsage", "number", ["number"]);
        this.QTS_RecoverableLeakCheck = this.module.cwrap("QTS_RecoverableLeakCheck", "number", []);
        this.QTS_BuildIsSanitizeLeak = this.module.cwrap("QTS_BuildIsSanitizeLeak", "number", []);
        this.QTS_RuntimeSetMaxStackSize = this.module.cwrap("QTS_RuntimeSetMaxStackSize", null, ["number", "number"]);
        this.QTS_GetUndefined = this.module.cwrap("QTS_GetUndefined", "number", []);
        this.QTS_GetNull = this.module.cwrap("QTS_GetNull", "number", []);
        this.QTS_GetFalse = this.module.cwrap("QTS_GetFalse", "number", []);
        this.QTS_GetTrue = this.module.cwrap("QTS_GetTrue", "number", []);
        this.QTS_NewHostRef = this.module.cwrap("QTS_NewHostRef", "number", ["number", "number"]);
        this.QTS_GetHostRefId = this.module.cwrap("QTS_GetHostRefId", "number", ["number"]);
        this.QTS_NewRuntime = this.module.cwrap("QTS_NewRuntime", "number", []);
        this.QTS_FreeRuntime = this.module.cwrap("QTS_FreeRuntime", null, ["number"]);
        this.QTS_NewContext = this.module.cwrap("QTS_NewContext", "number", ["number", "number"]);
        this.QTS_FreeContext = this.module.cwrap("QTS_FreeContext", null, ["number"]);
        this.QTS_FreeValuePointer = this.module.cwrap("QTS_FreeValuePointer", null, ["number", "number"]);
        this.QTS_FreeValuePointerRuntime = this.module.cwrap("QTS_FreeValuePointerRuntime", null, ["number", "number"]);
        this.QTS_FreeVoidPointer = this.module.cwrap("QTS_FreeVoidPointer", null, ["number", "number"]);
        this.QTS_FreeCString = this.module.cwrap("QTS_FreeCString", null, ["number", "number"]);
        this.QTS_DupValuePointer = this.module.cwrap("QTS_DupValuePointer", "number", ["number", "number"]);
        this.QTS_NewObject = this.module.cwrap("QTS_NewObject", "number", ["number"]);
        this.QTS_NewObjectProto = this.module.cwrap("QTS_NewObjectProto", "number", ["number", "number"]);
        this.QTS_NewArray = this.module.cwrap("QTS_NewArray", "number", ["number"]);
        this.QTS_NewArrayBuffer = this.module.cwrap("QTS_NewArrayBuffer", "number", ["number", "number", "number"]);
        this.QTS_NewFloat64 = this.module.cwrap("QTS_NewFloat64", "number", ["number", "number"]);
        this.QTS_GetFloat64 = this.module.cwrap("QTS_GetFloat64", "number", ["number", "number"]);
        this.QTS_NewString = this.module.cwrap("QTS_NewString", "number", ["number", "number"]);
        this.QTS_GetString = this.module.cwrap("QTS_GetString", "number", ["number", "number"]);
        this.QTS_GetArrayBuffer = this.module.cwrap("QTS_GetArrayBuffer", "number", ["number", "number"]);
        this.QTS_GetArrayBufferLength = this.module.cwrap("QTS_GetArrayBufferLength", "number", ["number", "number"]);
        this.QTS_NewSymbol = this.module.cwrap("QTS_NewSymbol", "number", ["number", "number", "number"]);
        this.QTS_GetSymbolDescriptionOrKey = this.module.cwrap("QTS_GetSymbolDescriptionOrKey", "number", ["number", "number"]);
        this.QTS_IsGlobalSymbol = this.module.cwrap("QTS_IsGlobalSymbol", "number", ["number", "number"]);
        this.QTS_IsJobPending = this.module.cwrap("QTS_IsJobPending", "number", ["number"]);
        this.QTS_ExecutePendingJob = this.module.cwrap("QTS_ExecutePendingJob", "number", ["number", "number", "number"]);
        this.QTS_GetProp = this.module.cwrap("QTS_GetProp", "number", ["number", "number", "number"]);
        this.QTS_GetPropNumber = this.module.cwrap("QTS_GetPropNumber", "number", ["number", "number", "number"]);
        this.QTS_SetProp = this.module.cwrap("QTS_SetProp", null, ["number", "number", "number", "number"]);
        this.QTS_DefineProp = this.module.cwrap("QTS_DefineProp", null, ["number", "number", "number", "number", "number", "number", "boolean", "boolean", "boolean"]);
        this.QTS_GetOwnPropertyNames = this.module.cwrap("QTS_GetOwnPropertyNames", "number", ["number", "number", "number", "number", "number"]);
        this.QTS_Call = this.module.cwrap("QTS_Call", "number", ["number", "number", "number", "number", "number"]);
        this.QTS_ResolveException = this.module.cwrap("QTS_ResolveException", "number", ["number", "number"]);
        this.QTS_Dump = this.module.cwrap("QTS_Dump", "number", ["number", "number"]);
        this.QTS_Eval = this.module.cwrap("QTS_Eval", "number", ["number", "number", "number", "string", "number", "number"]);
        this.QTS_GetModuleNamespace = this.module.cwrap("QTS_GetModuleNamespace", "number", ["number", "number"]);
        this.QTS_Typeof = this.module.cwrap("QTS_Typeof", "number", ["number", "number"]);
        this.QTS_GetLength = this.module.cwrap("QTS_GetLength", "number", ["number", "number", "number"]);
        this.QTS_IsEqual = this.module.cwrap("QTS_IsEqual", "number", ["number", "number", "number", "number"]);
        this.QTS_GetGlobalObject = this.module.cwrap("QTS_GetGlobalObject", "number", ["number"]);
        this.QTS_NewPromiseCapability = this.module.cwrap("QTS_NewPromiseCapability", "number", ["number", "number"]);
        this.QTS_PromiseState = this.module.cwrap("QTS_PromiseState", "number", ["number", "number"]);
        this.QTS_PromiseResult = this.module.cwrap("QTS_PromiseResult", "number", ["number", "number"]);
        this.QTS_TestStringArg = this.module.cwrap("QTS_TestStringArg", null, ["string"]);
        this.QTS_GetDebugLogEnabled = this.module.cwrap("QTS_GetDebugLogEnabled", "number", ["number"]);
        this.QTS_SetDebugLogEnabled = this.module.cwrap("QTS_SetDebugLogEnabled", null, ["number", "number"]);
        this.QTS_BuildIsDebug = this.module.cwrap("QTS_BuildIsDebug", "number", []);
        this.QTS_BuildIsAsyncify = this.module.cwrap("QTS_BuildIsAsyncify", "number", []);
        this.QTS_NewFunction = this.module.cwrap("QTS_NewFunction", "number", ["number", "string", "number", "boolean", "number"]);
        this.QTS_ArgvGetJSValueConstPointer = this.module.cwrap("QTS_ArgvGetJSValueConstPointer", "number", ["number", "number"]);
        this.QTS_RuntimeEnableInterruptHandler = this.module.cwrap("QTS_RuntimeEnableInterruptHandler", null, ["number"]);
        this.QTS_RuntimeDisableInterruptHandler = this.module.cwrap("QTS_RuntimeDisableInterruptHandler", null, ["number"]);
        this.QTS_RuntimeEnableModuleLoader = this.module.cwrap("QTS_RuntimeEnableModuleLoader", null, ["number", "number"]);
        this.QTS_RuntimeDisableModuleLoader = this.module.cwrap("QTS_RuntimeDisableModuleLoader", null, ["number"]);
        this.QTS_bjson_encode = this.module.cwrap("QTS_bjson_encode", "number", ["number", "number"]);
        this.QTS_bjson_decode = this.module.cwrap("QTS_bjson_decode", "number", ["number", "number"]);
      }
    };
  }
});

// node_modules/@jitl/quickjs-ng-wasmfile-release-sync/dist/emscripten-module.cloudflare.cjs
var require_emscripten_module_cloudflare = __commonJS({
  "node_modules/@jitl/quickjs-ng-wasmfile-release-sync/dist/emscripten-module.cloudflare.cjs"(exports, module) {
    var QuickJSRaw = (() => {
      var _scriptName = globalThis.document?.currentScript?.src;
      return async function(moduleArg = {}) {
        var moduleRtn;
        var c = moduleArg;
        function n(a) {
          a = { log: a || function() {
          } };
          for (const d of n.Qa) d(a);
          return c.quickJSEmscriptenExtensions = a;
        }
        n.Qa = [];
        c.quickjsEmscriptenInit = n;
        n.Qa.push((a) => {
          a.getWasmMemory = function() {
            return q;
          };
        });
        var r = "./this.program", t = "", u;
        try {
          t = new URL(".", _scriptName).href;
        } catch {
        }
        u = async (a) => {
          a = await fetch(a, { credentials: "same-origin" });
          if (a.ok) return a.arrayBuffer();
          throw Error(a.status + " : " + a.url);
        };
        var v = console.log.bind(console), w = console.error.bind(console), y, z = false, A, B, C, D, E, F, G, H, I = false;
        function J() {
          var a = q.buffer;
          c.HEAP8 = D = new Int8Array(a);
          new Int16Array(a);
          c.HEAPU8 = E = new Uint8Array(a);
          new Uint16Array(a);
          F = new Int32Array(a);
          G = new Uint32Array(a);
          new Float32Array(a);
          new Float64Array(a);
          H = new BigInt64Array(a);
          new BigUint64Array(a);
        }
        function K(a) {
          c.onAbort?.(a);
          a = "Aborted(" + a + ")";
          w(a);
          z = true;
          a = new WebAssembly.RuntimeError(a + ". Build with -sASSERTIONS for more info.");
          C?.(a);
          throw a;
        }
        var L;
        async function aa(a) {
          if (!y) try {
            var d = await u(a);
            return new Uint8Array(d);
          } catch {
          }
          if (a == L && y) a = new Uint8Array(y);
          else throw "both async and sync fetching of the wasm failed";
          return a;
        }
        async function ba(a, d) {
          try {
            var b = await aa(a);
            return await WebAssembly.instantiate(b, d);
          } catch (e) {
            w(`failed to asynchronously prepare wasm: ${e}`), K(e);
          }
        }
        async function ca(a) {
          var d = L;
          if (!y) try {
            var b = fetch(d, { credentials: "same-origin" });
            return await WebAssembly.instantiateStreaming(b, a);
          } catch (e) {
            w(`wasm streaming compile failed: ${e}`), w("falling back to ArrayBuffer instantiation");
          }
          return ba(d, a);
        }
        class M {
          name = "ExitStatus";
          constructor(a) {
            this.message = `Program terminated with exit(${a})`;
            this.status = a;
          }
        }
        var N = (a) => {
          for (; 0 < a.length; ) a.shift()(c);
        }, O = [], P = [], da = () => {
          var a = c.preRun.shift();
          P.push(a);
        }, Q = true, q, ea = new TextDecoder(), fa = (a, d, b, e) => {
          b = d + b;
          if (e) return b;
          for (; a[d] && !(d >= b); ) ++d;
          return d;
        }, R = (a, d, b) => a ? ea.decode(E.subarray(a, fa(E, a, d, b))) : "", S = 0, ha = [0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335], ia = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334], T = {}, ja = (a) => {
          if (!(a instanceof M || "unwind" == a)) throw a;
        }, ka = (a) => {
          A = a;
          Q || 0 < S || (c.onExit?.(a), z = true);
          throw new M(a);
        }, la = (a) => {
          if (!z) try {
            a();
          } catch (d) {
            ja(d);
          } finally {
            if (!(Q || 0 < S)) try {
              A = a = A, ka(a);
            } catch (d) {
              ja(d);
            }
          }
        }, U = (a, d, b) => {
          var e = E;
          if (!(0 < b)) return 0;
          var f = d;
          b = d + b - 1;
          for (var g = 0; g < a.length; ++g) {
            var h = a.codePointAt(g);
            if (127 >= h) {
              if (d >= b) break;
              e[d++] = h;
            } else if (2047 >= h) {
              if (d + 1 >= b) break;
              e[d++] = 192 | h >> 6;
              e[d++] = 128 | h & 63;
            } else if (65535 >= h) {
              if (d + 2 >= b) break;
              e[d++] = 224 | h >> 12;
              e[d++] = 128 | h >> 6 & 63;
              e[d++] = 128 | h & 63;
            } else {
              if (d + 3 >= b) break;
              e[d++] = 240 | h >> 18;
              e[d++] = 128 | h >> 12 & 63;
              e[d++] = 128 | h >> 6 & 63;
              e[d++] = 128 | h & 63;
              g++;
            }
          }
          e[d] = 0;
          return d - f;
        }, V = {}, ma = () => {
          if (!W) {
            var a = {
              USER: "web_user",
              LOGNAME: "web_user",
              PATH: "/",
              PWD: "/",
              HOME: "/home/web_user",
              LANG: (globalThis.navigator?.language ?? "C").replace("-", "_") + ".UTF-8",
              _: r || "./this.program"
            }, d;
            for (d in V) void 0 === V[d] ? delete a[d] : a[d] = V[d];
            var b = [];
            for (d in a) b.push(`${d}=${a[d]}`);
            W = b;
          }
          return W;
        }, W, X = (a) => {
          for (var d = 0, b = 0; b < a.length; ++b) {
            var e = a.charCodeAt(b);
            127 >= e ? d++ : 2047 >= e ? d += 2 : 55296 <= e && 57343 >= e ? (d += 4, ++b) : d += 3;
          }
          return d;
        }, na = [null, [], []], qa = (a, d, b, e) => {
          var f = { string: (k) => {
            var l = 0;
            if (null !== k && void 0 !== k && 0 !== k) {
              l = X(k) + 1;
              var p = Y(l);
              U(k, p, l);
              l = p;
            }
            return l;
          }, array: (k) => {
            var l = Y(k.length);
            D.set(k, l);
            return l;
          } };
          a = c["_" + a];
          var g = [], h = 0;
          if (e) for (var m = 0; m < e.length; m++) {
            var x = f[b[m]];
            x ? (0 === h && (h = oa()), g[m] = x(e[m])) : g[m] = e[m];
          }
          b = a(...g);
          return b = (function(k) {
            0 !== h && pa(h);
            return "string" === d ? R(k) : "boolean" === d ? !!k : k;
          })(b);
        };
        c.wasmMemory ? q = c.wasmMemory : q = new WebAssembly.Memory({ initial: (c.INITIAL_MEMORY || 16777216) / 65536, maximum: 32768 });
        J();
        c.noExitRuntime && (Q = c.noExitRuntime);
        c.print && (v = c.print);
        c.printErr && (w = c.printErr);
        c.wasmBinary && (y = c.wasmBinary);
        c.thisProgram && (r = c.thisProgram);
        if (c.preInit) for ("function" == typeof c.preInit && (c.preInit = [c.preInit]); 0 < c.preInit.length; ) c.preInit.shift()();
        c.cwrap = (a, d, b, e) => {
          var f = !b || b.every((g) => "number" === g || "boolean" === g);
          return "string" !== d && f && !e ? c["_" + a] : (...g) => qa(a, d, b, g);
        };
        c.UTF8ToString = R;
        c.stringToUTF8 = (a, d, b) => U(a, d, b);
        c.lengthBytesUTF8 = X;
        var ra, pa, Y, oa, sa = { b: (a, d, b, e) => K(`Assertion failed: ${R(a)}, at: ` + [d ? R(d) : "unknown filename", b, e ? R(e) : "unknown function"]), r: () => K(""), l: () => {
          Q = false;
          S = 0;
        }, m: function(a, d) {
          a = -9007199254740992 > a || 9007199254740992 < a ? NaN : Number(a);
          a = new Date(1e3 * a);
          F[d >> 2] = a.getSeconds();
          F[d + 4 >> 2] = a.getMinutes();
          F[d + 8 >> 2] = a.getHours();
          F[d + 12 >> 2] = a.getDate();
          F[d + 16 >> 2] = a.getMonth();
          F[d + 20 >> 2] = a.getFullYear() - 1900;
          F[d + 24 >> 2] = a.getDay();
          var b = a.getFullYear();
          F[d + 28 >> 2] = (0 !== b % 4 || 0 === b % 100 && 0 !== b % 400 ? ia : ha)[a.getMonth()] + a.getDate() - 1 | 0;
          F[d + 36 >> 2] = -(60 * a.getTimezoneOffset());
          b = new Date(a.getFullYear(), 6, 1).getTimezoneOffset();
          var e = new Date(a.getFullYear(), 0, 1).getTimezoneOffset();
          F[d + 32 >> 2] = (b != e && a.getTimezoneOffset() == Math.min(e, b)) | 0;
        }, j: (a, d) => {
          T[a] && (clearTimeout(T[a].id), delete T[a]);
          if (!d) return 0;
          var b = setTimeout(() => {
            delete T[a];
            la(() => ra(a, performance.now()));
          }, d);
          T[a] = { id: b, Ra: d };
          return 0;
        }, n: (a, d, b, e) => {
          var f = (/* @__PURE__ */ new Date()).getFullYear(), g = new Date(f, 0, 1).getTimezoneOffset();
          f = new Date(f, 6, 1).getTimezoneOffset();
          G[a >> 2] = 60 * Math.max(g, f);
          F[d >> 2] = Number(g != f);
          d = (h) => {
            var m = Math.abs(h);
            return `UTC${0 <= h ? "-" : "+"}${String(Math.floor(m / 60)).padStart(2, "0")}${String(m % 60).padStart(2, "0")}`;
          };
          a = d(g);
          d = d(f);
          f < g ? (U(a, b, 17), U(d, e, 17)) : (U(a, e, 17), U(d, b, 17));
        }, q: function(a, d, b) {
          if (!(0 <= a && 3 >= a)) return 28;
          H[b >> 3] = BigInt(Math.round(1e6 * (0 === a ? Date.now() : performance.now())));
          return 0;
        }, p: () => Date.now(), k: (a) => {
          var d = E.length;
          a >>>= 0;
          if (2147483648 < a) return false;
          for (var b = 1; 4 >= b; b *= 2) {
            var e = d * (1 + 0.2 / b);
            e = Math.min(e, a + 100663296);
            a: {
              e = (Math.min(
                2147483648,
                65536 * Math.ceil(Math.max(a, e) / 65536)
              ) - q.buffer.byteLength + 65535) / 65536 | 0;
              try {
                q.grow(e);
                J();
                var f = 1;
                break a;
              } catch (g) {
              }
              f = void 0;
            }
            if (f) return true;
          }
          return false;
        }, e: (a, d) => {
          var b = 0, e = 0, f;
          for (f of ma()) {
            var g = d + b;
            G[a + e >> 2] = g;
            b += U(f, g, Infinity) + 1;
            e += 4;
          }
          return 0;
        }, f: (a, d) => {
          var b = ma();
          G[a >> 2] = b.length;
          a = 0;
          for (var e of b) a += X(e) + 1;
          G[d >> 2] = a;
          return 0;
        }, d: () => 52, o: function() {
          return 70;
        }, c: (a, d, b, e) => {
          for (var f = 0, g = 0; g < b; g++) {
            var h = G[d >> 2], m = G[d + 4 >> 2];
            d += 8;
            for (var x = 0; x < m; x++) {
              var k = a, l = E[h + x], p = na[k];
              0 === l || 10 === l ? (k = 1 === k ? v : w, l = fa(p, 0), l = ea.decode(p.buffer ? p.subarray(0, l) : new Uint8Array(p.slice(0, l))), k(l), p.length = 0) : p.push(l);
            }
            f += m;
          }
          G[e >> 2] = f;
          return 0;
        }, a: q, s: ka, t: function(a, d, b, e, f) {
          return c.callbacks.callFunction(void 0, a, d, b, e, f);
        }, i: function(a) {
          return c.callbacks.shouldInterrupt(void 0, a);
        }, h: function(a, d, b) {
          b = R(b);
          return c.callbacks.loadModuleSource(void 0, a, d, b);
        }, g: function(a, d, b, e) {
          b = R(b);
          e = R(e);
          return c.callbacks.normalizeModule(void 0, a, d, b, e);
        }, u: function(a, d) {
          c.callbacks.freeHostRef(void 0, a, d);
        } }, Z;
        Z = await (async function() {
          function a(b) {
            b = Z = b.exports;
            c._malloc = b.w;
            c._QTS_Throw = b.x;
            c._QTS_NewError = b.y;
            c._QTS_RuntimeSetMemoryLimit = b.z;
            c._QTS_RuntimeComputeMemoryUsage = b.A;
            c._QTS_RuntimeDumpMemoryUsage = b.B;
            c._QTS_RecoverableLeakCheck = b.C;
            c._QTS_BuildIsSanitizeLeak = b.D;
            c._QTS_RuntimeSetMaxStackSize = b.E;
            c._QTS_GetUndefined = b.F;
            c._QTS_GetNull = b.G;
            c._QTS_GetFalse = b.H;
            c._QTS_GetTrue = b.I;
            c._QTS_NewHostRef = b.J;
            c._QTS_GetHostRefId = b.K;
            c._QTS_NewRuntime = b.L;
            c._QTS_FreeRuntime = b.M;
            c._free = b.N;
            c._QTS_NewContext = b.O;
            c._QTS_FreeContext = b.P;
            c._QTS_FreeValuePointer = b.Q;
            c._QTS_FreeValuePointerRuntime = b.R;
            c._QTS_FreeVoidPointer = b.S;
            c._QTS_FreeCString = b.T;
            c._QTS_DupValuePointer = b.U;
            c._QTS_NewObject = b.V;
            c._QTS_NewObjectProto = b.W;
            c._QTS_NewArray = b.X;
            c._QTS_NewArrayBuffer = b.Y;
            c._QTS_NewFloat64 = b.Z;
            c._QTS_GetFloat64 = b._;
            c._QTS_NewString = b.$;
            c._QTS_GetString = b.aa;
            c._QTS_GetArrayBuffer = b.ba;
            c._QTS_GetArrayBufferLength = b.ca;
            c._QTS_NewSymbol = b.da;
            c._QTS_GetSymbolDescriptionOrKey = b.ea;
            c._QTS_IsGlobalSymbol = b.fa;
            c._QTS_IsJobPending = b.ga;
            c._QTS_ExecutePendingJob = b.ha;
            c._QTS_GetProp = b.ia;
            c._QTS_GetPropNumber = b.ja;
            c._QTS_SetProp = b.ka;
            c._QTS_DefineProp = b.la;
            c._QTS_GetOwnPropertyNames = b.ma;
            c._QTS_Call = b.na;
            c._QTS_ResolveException = b.oa;
            c._QTS_Dump = b.pa;
            c._QTS_Eval = b.qa;
            c._QTS_GetModuleNamespace = b.ra;
            c._QTS_Typeof = b.sa;
            c._QTS_GetLength = b.ta;
            c._QTS_IsEqual = b.ua;
            c._QTS_GetGlobalObject = b.va;
            c._QTS_NewPromiseCapability = b.wa;
            c._QTS_PromiseState = b.xa;
            c._QTS_PromiseResult = b.ya;
            c._QTS_TestStringArg = b.za;
            c._QTS_GetDebugLogEnabled = b.Aa;
            c._QTS_SetDebugLogEnabled = b.Ba;
            c._QTS_BuildIsDebug = b.Ca;
            c._QTS_BuildIsAsyncify = b.Da;
            c._QTS_NewFunction = b.Ea;
            c._QTS_ArgvGetJSValueConstPointer = b.Fa;
            c._QTS_RuntimeEnableInterruptHandler = b.Ga;
            c._QTS_RuntimeDisableInterruptHandler = b.Ha;
            c._QTS_RuntimeEnableModuleLoader = b.Ia;
            c._QTS_RuntimeDisableModuleLoader = b.Ja;
            c._QTS_bjson_encode = b.Ka;
            c._QTS_bjson_decode = b.La;
            ra = b.Ma;
            pa = b.Na;
            Y = b.Oa;
            oa = b.Pa;
            return Z;
          }
          var d = { a: sa };
          if (c.instantiateWasm) return new Promise((b) => {
            c.instantiateWasm(d, (e, f) => {
              b(a(e, f));
            });
          });
          L ??= c.locateFile ? c.locateFile("emscripten-module.wasm", t) : t + "emscripten-module.wasm";
          return a((await ca(d)).instance);
        })();
        (function() {
          function a() {
            c.calledRun = true;
            if (!z) {
              I = true;
              Z.v();
              B?.(c);
              c.onRuntimeInitialized?.();
              if (c.postRun) for ("function" == typeof c.postRun && (c.postRun = [c.postRun]); c.postRun.length; ) {
                var d = c.postRun.shift();
                O.push(d);
              }
              N(O);
            }
          }
          if (c.preRun) for ("function" == typeof c.preRun && (c.preRun = [c.preRun]); c.preRun.length; ) da();
          N(P);
          c.setStatus ? (c.setStatus("Running..."), setTimeout(() => {
            setTimeout(() => c.setStatus(""), 1);
            a();
          }, 1)) : a();
        })();
        I ? moduleRtn = c : moduleRtn = new Promise((a, d) => {
          B = a;
          C = d;
        });
        ;
        return moduleRtn;
      };
    })();
    if (typeof exports === "object" && typeof module === "object") {
      module.exports = QuickJSRaw;
      module.exports.default = QuickJSRaw;
    } else if (typeof define === "function" && define["amd"]) define([], () => QuickJSRaw);
  }
});

// node_modules/quickjs-emscripten-core/dist/index.mjs
init_chunk_V2S4ZYJR();
init_dist();
async function newQuickJSWASMModuleFromVariant(variantOrPromise) {
  let variant2 = smartUnwrap(await variantOrPromise), [wasmModuleLoader, QuickJSFFI2, { QuickJSWASMModule: QuickJSWASMModule2 }] = await Promise.all([variant2.importModuleLoader().then(smartUnwrap), variant2.importFFI(), Promise.resolve().then(() => (init_module_ES6BEMUI(), module_ES6BEMUI_exports)).then(smartUnwrap)]), wasmModule2 = await wasmModuleLoader();
  wasmModule2.type = "sync";
  let ffi = new QuickJSFFI2(wasmModule2);
  return new QuickJSWASMModule2(wasmModule2, ffi);
}
function smartUnwrap(val) {
  return val && "default" in val && val.default ? val.default && "default" in val.default && val.default.default ? val.default.default : val.default : val;
}
function newVariant(baseVariant, options) {
  return { ...baseVariant, async importModuleLoader() {
    let moduleLoader = smartUnwrap(await baseVariant.importModuleLoader());
    return async function() {
      let moduleLoaderArg = options.emscriptenModule ? { ...options.emscriptenModule } : {}, log = options.log ?? ((...args) => debugLog("newVariant moduleLoader:", ...args)), tapValue = (message, val) => (log(...message, val), val), force = (val) => typeof val == "function" ? val() : val;
      (options.wasmLocation || options.wasmSourceMapLocation || options.locateFile) && (moduleLoaderArg.locateFile = (fileName, relativeTo) => {
        let args = { fileName, relativeTo };
        if (fileName.endsWith(".wasm") && options.wasmLocation !== void 0) return tapValue(["locateFile .wasm: provide wasmLocation", args], options.wasmLocation);
        if (fileName.endsWith(".map")) {
          if (options.wasmSourceMapLocation !== void 0) return tapValue(["locateFile .map: provide wasmSourceMapLocation", args], options.wasmSourceMapLocation);
          if (options.wasmLocation && !options.locateFile) return tapValue(["locateFile .map: infer from wasmLocation", args], options.wasmLocation + ".map");
        }
        return options.locateFile ? tapValue(["locateFile: use provided fn", args], options.locateFile(fileName, relativeTo)) : tapValue(["locateFile: unhandled, passthrough", args], fileName);
      }), options.wasmBinary && (moduleLoaderArg.wasmBinary = await force(options.wasmBinary)), options.wasmMemory && (moduleLoaderArg.wasmMemory = await force(options.wasmMemory));
      let optionsWasmModule = options.wasmModule, modulePromise;
      optionsWasmModule && (moduleLoaderArg.instantiateWasm = async (imports, onSuccess) => {
        modulePromise ?? (modulePromise = Promise.resolve(force(optionsWasmModule)));
        let wasmModule2 = await modulePromise;
        if (!wasmModule2) throw new QuickJSEmscriptenModuleError(`options.wasmModule returned ${String(wasmModule2)}`);
        let instance = await WebAssembly.instantiate(wasmModule2, imports);
        return onSuccess(instance), instance.exports;
      }), moduleLoaderArg.monitorRunDependencies = (left) => {
        log("monitorRunDependencies:", left);
      }, moduleLoaderArg.quickjsEmscriptenInit = () => newMockExtensions(log);
      let resultPromise = moduleLoader(moduleLoaderArg), extensions = moduleLoaderArg.quickjsEmscriptenInit?.(log);
      if (optionsWasmModule && extensions?.receiveWasmOffsetConverter && !extensions.existingWasmOffsetConverter) {
        let wasmBinary = await force(options.wasmBinary) ?? new ArrayBuffer(0);
        modulePromise ?? (modulePromise = Promise.resolve(force(optionsWasmModule)));
        let wasmModule2 = await modulePromise;
        if (!wasmModule2) throw new QuickJSEmscriptenModuleError(`options.wasmModule returned ${String(wasmModule2)}`);
        extensions.receiveWasmOffsetConverter(wasmBinary, wasmModule2);
      }
      if (extensions?.receiveSourceMapJSON) {
        let loadedSourceMapData = await force(options.wasmSourceMapData);
        typeof loadedSourceMapData == "string" ? extensions.receiveSourceMapJSON(JSON.parse(loadedSourceMapData)) : loadedSourceMapData ? extensions.receiveSourceMapJSON(loadedSourceMapData) : extensions.receiveSourceMapJSON({ version: 3, names: [], sources: [], mappings: "" });
      }
      return resultPromise;
    };
  } };
}
function newMockExtensions(log) {
  let mockMessage = "mock called, emscripten module may not be initialized yet";
  return { mock: true, removeRunDependency(name) {
    log(`${mockMessage}: removeRunDependency called:`, name);
  }, receiveSourceMapJSON(data) {
    log(`${mockMessage}: receiveSourceMapJSON called:`, data);
  }, WasmOffsetConverter: void 0, receiveWasmOffsetConverter(bytes, mod) {
    log(`${mockMessage}: receiveWasmOffsetConverter called:`, bytes, mod);
  } };
}

// node_modules/@jitl/quickjs-ng-wasmfile-release-sync/dist/index.mjs
var variant = { type: "sync", importFFI: () => Promise.resolve().then(() => (init_ffi(), ffi_exports)).then((mod) => mod.QuickJSFFI), importModuleLoader: () => Promise.resolve().then(() => __toESM(require_emscripten_module_cloudflare(), 1)).then((mod) => mod.default) };
var src_default = variant;

// guest.js
import wasmModule from "./quickjs.wasm";

// prelude.js
var prelude_default = '\n(function (host) {\n  "use strict";\n  const define = (target, key, value) =>\n    Object.defineProperty(target, key, { value, enumerable: true, writable: false, configurable: false });\n  const tools = Object.create(null);\n  for (const name of host.names()) {\n    const call = (args) => host.invoke(name, args === undefined ? {} : args, false);\n    const raw = (args) => host.invoke(name, args === undefined ? {} : args, true);\n    define(call, "raw", Object.freeze(raw));\n    define(call, "toolName", name);\n    define(tools, name, Object.freeze(call));\n  }\n  Object.freeze(tools);\n  define(globalThis, "tools", tools);\n  define(globalThis, "text", (value) => host.text(value));\n  define(globalThis, "searchTools", (query, options) =>\n    host.searchTools(query === undefined || query === null ? "" : String(query),\n      options && options.limit !== undefined && options.limit !== null ? Number(options.limit) : undefined));\n  define(globalThis, "describeTool", (name) => host.describeTool(String(name)));\n})(globalThis.__host);\ndelete globalThis.__host;\n';

// discovery.js
var encoder = new TextEncoder();
var tokens = (text) => text.match(/[\p{Alphabetic}\p{Number}]+/gu)?.map((s) => s.toLowerCase()) || [];
function discover(catalog, query, requested) {
  if (encoder.encode(query).length > 4096) throw new RangeError("search query exceeds 4096 bytes");
  const queryTokens = tokens(query);
  if (queryTokens.length > 32) throw new RangeError("search query exceeds 32 tokens");
  const terms = [...new Set(queryTokens)].sort();
  const limit = requested === void 0 ? 10 : Number.isFinite(requested) && requested >= 1 ? Math.min(50, Math.floor(requested)) : 1;
  const documents = catalog.map((entry) => {
    const frequencies = /* @__PURE__ */ new Map();
    const words = tokens(`${entry.namespace || ""} ${entry.description}`);
    for (const word of words) frequencies.set(word, (frequencies.get(word) || 0) + 1);
    return { entry, frequencies, length: words.length, names: tokens(entry.name) };
  });
  const average = documents.reduce((sum, d) => sum + d.length, 0) / Math.max(1, documents.length);
  const idfs = terms.map((term) => {
    const df = documents.filter((d) => d.frequencies.has(term) || d.names.includes(term)).length;
    return Math.log((documents.length - df + 0.5) / (df + 0.5) + 1);
  });
  return documents.map((d) => {
    let score = 0;
    terms.forEach((term, i) => {
      if (d.names.includes(term)) score += idfs[i] * 2.2;
      const tf = d.frequencies.get(term);
      if (tf) score += idfs[i] * (tf * 2.2) / (tf + 1.2 * (0.25 + 0.75 * d.length / Math.max(1, average)));
    });
    return { score, entry: d.entry };
  }).filter((d) => d.score > 0).sort((a, b) => b.score - a.score || compareUtf8(a.entry.name, b.entry.name)).slice(0, limit).map((d) => describe(d.entry, false));
}
function compareUtf8(a, b) {
  const left = encoder.encode(a), right = encoder.encode(b);
  for (let i = 0; i < Math.min(left.length, right.length); i++) if (left[i] !== right[i]) return left[i] - right[i];
  return left.length - right.length;
}
function describe(entry, full = true) {
  const result = { name: entry.name, description: entry.description };
  if (entry.namespace !== void 0) result.namespace = entry.namespace;
  if (full) {
    result.inputSchema = entry.input_schema;
    if (entry.output_schema !== void 0) result.outputSchema = entry.output_schema;
  }
  return result;
}

// guest.js
var MAX_TRANSPORT = 16 * 1024 * 1024;
var MAX_INTERRUPTS = 1e4;
var MAX_JOBS = 1e5;
var plain = (message) => ({ message, line: null, column: null, stack: null });
var charSize = (char) => {
  const point = char.codePointAt(0);
  return point < 128 ? 1 : point < 2048 ? 2 : point < 65536 ? 3 : 4;
};
var size = (text) => {
  let bytes = 0;
  for (const char of text) bytes += charSize(char);
  return bytes;
};
var stringSize = (text) => {
  let bytes = 2;
  for (const char of text) {
    const point = char.codePointAt(0);
    bytes += char === '"' || char === "\\" || "\b	\n\f\r".includes(char) ? 2 : point < 32 || point >= 55296 && point <= 57343 ? 6 : charSize(char);
  }
  return bytes;
};
var wireSize = (value) => {
  if (typeof value === "string") return stringSize(value);
  if (value === null || typeof value !== "object") return JSON.stringify(value).length;
  if (Array.isArray(value)) return 2 + Math.max(0, value.length - 1) + value.reduce((n, v) => n + wireSize(v), 0);
  const fields = Object.entries(value);
  return 2 + Math.max(0, fields.length - 1) + fields.reduce((n, [key, v]) => n + stringSize(key) + 1 + wireSize(v), 0);
};
var cut = (text, limit) => {
  let result = "", length = 0;
  for (const char of text) {
    const n = size(char);
    if (length + n > limit) break;
    result += char;
    length += n;
  }
  return result;
};
var equal = (a, b) => {
  if (a === b) return true;
  if (!a || !b || typeof a !== "object" || typeof b !== "object") return false;
  const ak = Object.keys(a).sort(), bk = Object.keys(b).sort();
  return ak.length === bk.length && ak.every((k, i) => k === bk[i] && equal(a[k], b[k]));
};
async function exchange(request) {
  if (request.version !== 1) throw new Error("unsupported replay protocol");
  for (const value of [request.budget.work, request.budget.bytes, ...Object.values(request.limits).filter((v) => typeof v === "number")])
    if (!Number.isSafeInteger(value) || value <= 0) throw new Error("unsafe numeric limit");
  let work = 1, interrupts = 0, jobs = 0, interrupted = false, transportExceeded = false;
  const responseLimit = Math.min(MAX_TRANSPORT, request.budget.bytes);
  let retainedBytes = wireSize({ version: 1, batches: [], usage: { work: 1, response_bytes: 0, elapsed: { secs: 0, nanos: 0 } } });
  const checkTransport = (extra) => {
    if (transportExceeded || extra > responseLimit - retainedBytes) {
      transportExceeded = true;
      throw { budget: true, message: "response exceeds transport limit" };
    }
  };
  const charge = (extra) => {
    checkTransport(extra);
    retainedBytes += extra;
  };
  checkTransport(0);
  const qjs = await newQuickJSWASMModuleFromVariant(newVariant(src_default, { wasmModule }));
  const runtime = qjs.newRuntime();
  runtime.setMemoryLimit(request.limits.memory_bytes);
  runtime.setMaxStackSize(64 * 1024);
  runtime.setInterruptHandler(() => {
    work++;
    return interrupted ||= transportExceeded || ++interrupts > MAX_INTERRUPTS || work > request.budget.work;
  });
  const context = runtime.newContext();
  const handles = [], pending = /* @__PURE__ */ new Map(), batches = [];
  const output = { text: "", truncated: false };
  let ordinal = 0, overBudget = 0, events = [];
  const keep = (handle) => {
    handles.push(handle);
    return handle;
  };
  const diagnostic = (error) => {
    const budget = Math.min(request.limits.message_bytes, request.limits.output_bytes, 8192);
    let message = "error", stack = "";
    const read = (key, limit) => {
      const handle = context.getProp(error, key);
      try {
        return cut(guestString(handle), limit);
      } finally {
        handle.dispose();
      }
    };
    try {
      if (context.typeof(error) === "object") {
        const name = read("name", Math.floor(budget / 4));
        message = name && name !== "Error" ? name + ": " : "";
        message = cut(message, budget) + read("message", Math.max(0, budget - size(message)));
        stack = read("stack", Math.max(0, budget - size(message)));
      } else message = cut(guestString(error), budget);
    } catch (_) {
    }
    const match = /script:(\d+)(?::(\d+))?/.exec(stack);
    return {
      message,
      line: match ? Math.max(1, Number(match[1]) - 1) : null,
      column: match && match[2] ? Number(match[2]) : null,
      stack: stack || null
    };
  };
  const unwrap = (result) => {
    if (result.error) {
      const error = result.error;
      try {
        throw interrupted ? { ...plain("the script exceeded its work limit"), budget: true } : diagnostic(error);
      } finally {
        error.dispose();
      }
    }
    return result.value;
  };
  const json = keep(context.getProp(context.global, "JSON"));
  const stringify = keep(context.getProp(json, "stringify"));
  const parse = keep(context.getProp(json, "parse"));
  const string = keep(context.getProp(context.global, "String"));
  const serialize = (value) => {
    const handle = unwrap(context.callFunction(stringify, context.undefined, value));
    try {
      if (context.typeof(handle) === "undefined") return void 0;
      const length = context.getProp(handle, "length");
      try {
        checkTransport(context.getNumber(length));
      } finally {
        length.dispose();
      }
      return context.getString(handle);
    } finally {
      handle.dispose();
    }
  };
  const guestString = (value) => {
    const converted = unwrap(context.callFunction(string, context.undefined, value));
    try {
      return JSON.parse(serialize(converted));
    } finally {
      converted.dispose();
    }
  };
  const fromJson = (text) => {
    const handle = context.newString(text);
    try {
      return unwrap(context.callFunction(parse, context.undefined, handle));
    } finally {
      handle.dispose();
    }
  };
  const toGuest = (value) => {
    const text = JSON.stringify(value);
    if (size(text) > request.limits.message_bytes) throw new RangeError("discovery result exceeds the byte or nesting limit");
    let depth = 0, quoted = false, escaped = false;
    for (const char of text) {
      if (quoted) {
        if (escaped) escaped = false;
        else if (char === "\\") escaped = true;
        else if (char === '"') quoted = false;
      } else if (char === '"') quoted = true;
      else if (char === "[" || char === "{") {
        if (++depth > 64) throw new RangeError("discovery result exceeds the byte or nesting limit");
      } else if (char === "]" || char === "}") depth--;
    }
    return fromJson(text);
  };
  const guestError = (properties) => {
    const handle = context.newError("");
    for (const [key, value] of Object.entries(properties)) {
      const property = fromJson(JSON.stringify(value));
      context.setProp(handle, key, property);
      property.dispose();
    }
    return handle;
  };
  const toolError = (error) => guestError({ name: "CodeModeToolError", message: error.message, kind: error.kind, status: error.status, ...error.tool ? { tool: error.tool } : {} });
  let primary;
  try {
    const host = keep(context.newObject());
    const callback = (name, fn) => {
      const handle = context.newFunction(name, (...args) => {
        try {
          checkTransport(0);
          return fn(...args);
        } catch (error) {
          return { error: guestError({ name: error instanceof RangeError ? "RangeError" : "Error", message: error.message || "guest bridge failed" }) };
        }
      });
      context.setProp(host, name, handle);
      handle.dispose();
    };
    callback("names", () => fromJson(JSON.stringify(request.catalog.map((entry) => entry.name))));
    callback("invoke", (nameHandle, args, rawHandle) => {
      const name = guestString(nameHandle), raw = context.getNumber(rawHandle) !== 0;
      const promise = context.newPromise();
      const event = (value) => {
        try {
          charge(wireSize(value) + (events.length ? 1 : 0));
        } catch (error) {
          promise.dispose();
          throw error;
        }
        events.push(value);
      };
      const reject = (kind, message, status = "rejected", id2) => {
        if (id2 !== void 0) event({ Rejected: { ordinal: id2, name, kind } });
        const error = toolError({ kind, message, status, tool: name });
        const result = promise.handle.dup();
        promise.reject(error);
        error.dispose();
        promise.dispose();
        return result;
      };
      if (ordinal >= request.limits.max_calls) {
        overBudget = Math.min(4294967295, overBudget + 1);
        return reject("other", `call budget of ${request.limits.max_calls} tool calls is exhausted`);
      }
      const id = ordinal++;
      let argumentsJson;
      try {
        argumentsJson = serialize(args);
      } catch (_) {
      }
      if (argumentsJson === void 0 || size(argumentsJson) > request.limits.message_bytes) {
        if (argumentsJson === void 0) return reject("invalid_args", "tool arguments must be a JSON-serializable value", "rejected", id);
        return reject("invalid_args", `tool arguments are ${size(argumentsJson)} bytes; the limit is ${request.limits.message_bytes} bytes`, "size", id);
      }
      event({ Call: { ordinal: id, name, arguments: argumentsJson, raw } });
      pending.set(id, promise);
      return promise.handle.dup();
    });
    callback("text", (value) => {
      const isString = context.typeof(value) === "string";
      const text = isString ? guestString(value) : serialize(value) ?? "undefined";
      const remaining = Math.max(0, request.limits.output_bytes - size(output.text));
      if (!isString && size(text) >= remaining)
        throw new RangeError(`text() value is ${size(text)} bytes but only ${Math.max(0, remaining - 1)} bytes of the ${request.limits.output_bytes} byte output budget remain`);
      if (!output.truncated) {
        const appended = size(text) < remaining ? text + "\n" : cut(text, remaining);
        checkTransport(stringSize(output.text) + stringSize(appended) - 2);
        output.text += appended;
        if (size(text) >= remaining) output.truncated = true;
      }
    });
    callback("describeTool", (nameHandle) => {
      const name = guestString(nameHandle);
      if (size(name) > 128) throw new RangeError("tool name exceeds 128 bytes");
      const entry = request.catalog.find((entry2) => entry2.name === name);
      return entry ? toGuest(describe(entry)) : context.null;
    });
    callback("searchTools", (query, limit) => toGuest(discover(request.catalog, guestString(query), !limit || context.typeof(limit) === "undefined" ? void 0 : context.getNumber(limit))));
    context.setProp(context.global, "__host", host);
    unwrap(context.evalCode(prelude_default, "prelude")).dispose();
    unwrap(context.evalCode(`delete globalThis.Date; delete globalThis.WeakRef; delete globalThis.FinalizationRegistry;
      Object.defineProperty(Math, "random", { value: () => { throw new Error("Math.random is unavailable in code mode"); }, writable: false, configurable: false });`, "restrictions")).dispose();
    let root, failure;
    try {
      root = keep(unwrap(context.evalCode(`(async () => {
${request.code}
})()`, "script")));
    } catch (error) {
      failure = error;
    }
    for (let index = 0; ; index++) {
      try {
        while (runtime.hasPendingJob()) {
          if (++jobs > MAX_JOBS || ++work > request.budget.work) {
            interrupted = true;
            throw plain("the script exceeded its job or work limit");
          }
          const result = runtime.executePendingJobs(1);
          if (result.error) unwrap(result);
        }
      } catch (error) {
        failure = error;
      }
      let boundary;
      const done = (status, returned, diagnostic2) => ({ Done: { status, output: { ...output }, returned, diagnostic: diagnostic2, over_budget_calls: overBudget } });
      if (failure || interrupted) boundary = done(interrupted ? "timed_out" : "script_error", null, failure || plain("the script exceeded its work limit"));
      else {
        const promise = context.getPromiseState(root);
        if (promise.type === "pending") boundary = pending.size ? { Idle: { output: { ...output } } } : done("stalled", null, plain("the script awaited a promise that no tool call or pending job can settle"));
        else {
          let returned = null, error = null;
          try {
            if (promise.type === "rejected") error = diagnostic(promise.error);
            else {
              const text = serialize(promise.value);
              if (text !== void 0) {
                if (size(text) > request.limits.message_bytes) throw plain(`the return value is ${size(text)} bytes; the limit is ${request.limits.message_bytes} bytes`);
                returned = text;
              }
            }
          } catch (failure2) {
            error = failure2;
          } finally {
            (promise.error || promise.value)?.dispose();
          }
          boundary = done(interrupted ? "timed_out" : error ? "script_error" : "completed", returned, error);
        }
      }
      if (transportExceeded) throw { budget: true, message: "response exceeds transport limit" };
      charge(wireSize({ events: [], boundary }) + (batches.length ? 1 : 0));
      const batch = { events, boundary };
      events = [];
      batches.push(batch);
      if (index === request.prefix.length) break;
      const expected = request.prefix[index];
      if (!equal(batch, expected.batch) || !boundary.Idle) throw new Error("replay prefix mismatch");
      const deferred = pending.get(expected.reply.ordinal);
      if (!deferred) throw new Error("unknown or already settled reply target");
      pending.delete(expected.reply.ordinal);
      try {
        const payload = expected.reply.payload;
        const value = payload.Resolve !== void 0 ? fromJson(payload.Resolve) : toolError(payload.Reject);
        try {
          if (payload.Resolve !== void 0) deferred.resolve(value);
          else deferred.reject(value);
        } finally {
          value.dispose();
        }
      } finally {
        deferred.dispose();
      }
    }
    charge(String(work).length - 1);
    return { version: 1, batches, usage: { work, response_bytes: 0, elapsed: { secs: 0, nanos: 0 } } };
  } catch (error) {
    primary = error;
    throw error;
  } finally {
    let cleanup;
    const dispose = (value) => {
      try {
        value.dispose();
      } catch (error) {
        cleanup ||= error;
      }
    };
    for (const deferred of pending.values()) dispose(deferred);
    for (const handle of handles.reverse()) if (handle.alive) dispose(handle);
    dispose(context);
    dispose(runtime);
    if (!primary && cleanup) throw cleanup;
  }
}
async function readBounded(body, limit) {
  if (!body) throw new Error("missing request body");
  const reader = body.getReader(), chunks = [];
  let length = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      if (value.byteLength > limit - length) throw new Error("request exceeds transport limit");
      length += value.byteLength;
      chunks.push(value);
    }
  } finally {
    await reader.cancel().catch(() => {
    });
    reader.releaseLock();
  }
  const bytes = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
}
var guest_default = {
  async fetch(request) {
    try {
      const input = JSON.parse(await readBounded(request.body, MAX_TRANSPORT));
      const response = JSON.stringify(await exchange(input));
      if (size(response) > Math.min(MAX_TRANSPORT, input.budget.bytes)) throw new Error("response exceeds transport limit");
      return new Response(response, { headers: { "Content-Type": "application/json" } });
    } catch (error) {
      return new Response("replay exchange failed", { status: error.budget ? 429 : 422 });
    }
  }
};
export {
  guest_default as default
};
