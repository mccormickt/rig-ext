const west = await tools.lookup({region: "west"});
const east = await tools.lookup({region: "east"});
if ("_meta" in west || "_meta" in east) throw new Error("MCP metadata reached the script");
const total = west.answer + east.answer;
text(`Available units: ${total}`);
return {total};
