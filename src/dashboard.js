import { createBullBoard } from '@bull-board/api';
import { BullMQAdapter } from '@bull-board/api/bullMQAdapter';
import { ExpressAdapter } from '@bull-board/express';
import { slotQueue } from './queue.js';

function basicAuthMiddleware(req, res, next) {
    const adminUser = process.env.ADMIN_USER || 'admin';
    const adminPass = process.env.ADMIN_PASS || 'admin';

    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Basic ')) {
        res.setHeader('WWW-Authenticate', 'Basic realm="SlotGuard Admin Dashboard"');
        return res.status(401).send('Authentication required');
    }

    const credentials = Buffer.from(authHeader.slice(6), 'base64').toString('utf-8');
    const [user, pass] = credentials.split(':');

    if (user === adminUser && pass === adminPass) {
        return next();
    }

    res.setHeader('WWW-Authenticate', 'Basic realm="SlotGuard Admin Dashboard"');
    return res.status(401).send('Invalid credentials');
}

export function setupBullBoard(app, basePath = '/admin/queues') {
    const serverAdapter = new ExpressAdapter();
    serverAdapter.setBasePath(basePath);

    createBullBoard({
        queues: [new BullMQAdapter(slotQueue)],
        serverAdapter,
    });

    app.use(basePath, basicAuthMiddleware, serverAdapter.getRouter());
    return serverAdapter;
}
