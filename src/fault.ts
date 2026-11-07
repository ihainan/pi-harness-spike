// Fault injection for kit negative controls only. PI_FAULT=a,b breaks a behaviour on purpose so we
// can confirm the kit reports FAIL; never set in a real run. Not counted as prototype functionality.
const faults = new Set((process.env.PI_FAULT ?? "").split(",").filter(Boolean));
export const fault = (name: string): boolean => faults.has(name);
