
(function (host) {
  "use strict";
  const define = (target, key, value) =>
    Object.defineProperty(target, key, { value, enumerable: true, writable: false, configurable: false });
  const tools = Object.create(null);
  for (const name of host.names()) {
    const call = (args) => host.invoke(name, args === undefined ? {} : args, false);
    const raw = (args) => host.invoke(name, args === undefined ? {} : args, true);
    define(call, "raw", Object.freeze(raw));
    define(call, "toolName", name);
    define(tools, name, Object.freeze(call));
  }
  Object.freeze(tools);
  define(globalThis, "tools", tools);
  define(globalThis, "text", (value) => host.text(value));
  define(globalThis, "searchTools", (query, options) =>
    host.searchTools(query === undefined || query === null ? "" : String(query),
      options && options.limit !== undefined && options.limit !== null ? Number(options.limit) : undefined));
  define(globalThis, "describeTool", (name) => host.describeTool(String(name)));
})(globalThis.__host);
delete globalThis.__host;
