// sseConnections.js
// A registry of currently-open SSE connections, keyed by userId.
// Lets any part of the app (the pub/sub message handler, specifically)
// find and write to a specific user's open response, if they have one.

export const sseClients = new Map();

export function addClient(userId, res) {
    sseClients.set(userId, res);
}

export function removeClient(userId) {
    sseClients.delete(userId);
}