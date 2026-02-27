// Quick Test Script for WASM Integration
// Paste this into browser console after loading a PLY file

// 1. Check if WASM is being used
console.log('%c=== WASM Integration Test ===', 'color: blue; font-weight: bold');

// Look for WASM initialization message
console.log('✓ Check for: "WASM module initialized successfully"');

// 2. Monitor performance
const originalConsoleLog = console.log;
let wasmLogs = [];

console.log = function(...args) {
    const message = args.join(' ');
    if (message.includes('[WASM]')) {
        wasmLogs.push(message);
    }
    originalConsoleLog.apply(console, args);
};

// 3. After loading a file, analyze performance
setTimeout(() => {
    console.log('%c=== WASM Performance Summary ===', 'color: green; font-weight: bold');
    wasmLogs.forEach(log => console.log(log));
    
    // Calculate speed
    const parseLog = wasmLogs.find(l => l.includes('Parsed'));
    if (parseLog) {
        const match = parseLog.match(/Parsed ([\d,]+) points in ([\d.]+)ms/);
        if (match) {
            const points = parseInt(match[1].replace(/,/g, ''));
            const time = parseFloat(match[2]);
            const pointsPerSecond = (points / time) * 1000;
            console.log(`%cParsing Speed: ${pointsPerSecond.toLocaleString()} points/second`, 'color: orange; font-weight: bold');
        }
    }
}, 5000);

// 4. Expected benchmarks (approximate)
console.log('%cExpected Performance:', 'color: purple; font-weight: bold');
console.log('• Small files (<1M points): 50-200ms');
console.log('• Medium files (1-5M points): 200-1000ms');
console.log('• Large files (5-10M points): 1-3 seconds');
console.log('• Very large files (>10M points): 3-10 seconds');
console.log('\n%cCompare to pure JS (2-10x slower)', 'color: gray');
