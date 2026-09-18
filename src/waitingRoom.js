// waitingRoom.js
// Shared logic for the virtual waiting room / admission control system.
// Used by BOTH server.js (join/status endpoints + admission-check middleware)
// and worker.js (the background admission cycle that lets people in).

export const WAITING_QUEUE_KEY = 'queue:waiting';
export const ADMITTED_KEY_PREFIX = 'queue:admitted:';
export const ADMITTED_TTL_SECONDS = 30;   // how long an admission stays valid once granted
export const ADMISSION_BATCH_SIZE = 5;    // how many users let in per cycle
export const ADMISSION_INTERVAL_MS = 3000; // how often a new batch is admitted

export function admittedKey(userId) {
    return `${ADMITTED_KEY_PREFIX}${userId}`;
}

// Middleware factory: blocks a request unless this user has been admitted.
// Only enforced when WAITING_ROOM_ENABLED=true, so normal dev/testing isn't
// disrupted unless the waiting room is deliberately turned on — mirroring
// how real systems only activate this during an actual demand spike.
export function createAdmissionMiddleware(redis) {
    return async function admissionMiddleware(req, res, next) {
        if (process.env.WAITING_ROOM_ENABLED !== 'true') {
            return next(); // waiting room OFF — behave exactly as before
        }

        const userId = req.user.id;
        const admitted = await redis.get(admittedKey(userId));

        if (admitted) {
            return next();
        }

        return res.status(403).json({
            error: 'You have not been admitted yet. Join the queue at POST /api/v1/queue/join and poll GET /api/v1/queue/status.'
        });
    };
}

// Runs one admission cycle: pops the earliest-waiting batch off the queue
// and grants each of them a short-lived "admitted" flag.
export async function runAdmissionCycle(connection) {
    const popped = await connection.zpopmin(WAITING_QUEUE_KEY, ADMISSION_BATCH_SIZE);
    // ioredis's zpopmin(key, count) returns a FLAT array:
    // [member1, score1, member2, score2, ...]

    if (popped.length === 0) {
        return []; // nobody waiting
    }

    const admittedUsers = [];
    for (let i = 0; i < popped.length; i += 2) {
        const userId = popped[i];
        await connection.set(admittedKey(userId), '1', 'EX', ADMITTED_TTL_SECONDS);
        admittedUsers.push(userId);
    }

    return admittedUsers;
}

export const ADMISSION_CHANNEL = 'queue:admissions';
export const REASSIGNMENT_CHANNEL = 'queue:reassignments';