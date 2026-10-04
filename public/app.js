// SlotGuard Frontend App JS - Warm Emerald Edition
document.addEventListener('DOMContentLoaded', () => {
    // --- Application State ---
    let currentUser = {
        userId: localStorage.getItem('slotguard_userId') || 'user_1',
        token: localStorage.getItem('slotguard_token') || null
    };

    let selectedPodId = '1';
    let slotsData = [];
    let eventSource = null;

    // --- DOM Elements ---
    const navTabs = document.querySelectorAll('.nav-tab');
    const tabPages = document.querySelectorAll('.tab-page');
    const userPills = document.querySelectorAll('.user-pill');
    const healthLabel = document.getElementById('healthLabel');

    // Pod Arena Elements
    const podsGrid = document.getElementById('podsGrid');
    const selectedPodNum = document.getElementById('selectedPodNum');
    const selectedPodBadge = document.getElementById('selectedPodBadge');
    const podHolderVal = document.getElementById('podHolderVal');
    const podTTLVal = document.getElementById('podTTLVal');
    const podWaitlistVal = document.getElementById('podWaitlistVal');
    const ttlMetaRow = document.getElementById('ttlMetaRow');

    // Action Controls
    const holdBtn = document.getElementById('holdBtn');
    const confirmBtn = document.getElementById('confirmBtn');
    const waitlistBtn = document.getElementById('waitlistBtn');
    const actionStatusBanner = document.getElementById('actionStatusBanner');

    // Circular Timer
    const countdownRingBox = document.getElementById('countdownRingBox');
    const ringFillCircle = document.getElementById('ringFillCircle');
    const ringTimeText = document.getElementById('ringTimeText');

    // Waiting Room Elements
    const queueValText = document.getElementById('queueValText');
    const queueSubText = document.getElementById('queueSubText');
    const joinQueueBtn = document.getElementById('joinQueueBtn');
    const checkQueueBtn = document.getElementById('checkQueueBtn');

    // SSE Terminal
    const sseTerminal = document.getElementById('sseTerminal');
    const sseLivePill = document.getElementById('sseLivePill');

    // Arena Sim Elements
    const fireSimBtn = document.getElementById('fireSimBtn');
    const simWinnerCount = document.getElementById('simWinnerCount');
    const simConflictCount = document.getElementById('simConflictCount');
    const simPrecisionVal = document.getElementById('simPrecisionVal');
    const simLatencyVal = document.getElementById('simLatencyVal');
    const vuArenaGrid = document.getElementById('vuArenaGrid');
    const simTerminal = document.getElementById('simTerminal');

    // AI Agent Chat Elements
    const chatForm = document.getElementById('chatForm');
    const chatInput = document.getElementById('chatInput');
    const chatFeed = document.getElementById('chatFeed');
    const promptChips = document.querySelectorAll('.prompt-chip');

    // --- Tab Switcher Logic ---
    window.switchTab = function(tabId) {
        console.log('[Nav] Switching tab to:', tabId);
        navTabs.forEach(tab => {
            if (tab.getAttribute('data-tab') === tabId) {
                tab.classList.add('active');
            } else {
                tab.classList.remove('active');
            }
        });

        tabPages.forEach(page => {
            if (page.id === `${tabId}-tab`) {
                page.classList.add('active');
                page.style.display = 'block';
            } else {
                page.classList.remove('active');
                page.style.display = 'none';
            }
        });
    };

    navTabs.forEach(tab => {
        tab.addEventListener('click', (e) => {
            e.preventDefault();
            const tabId = tab.getAttribute('data-tab');
            if (tabId) switchTab(tabId);
        });
    });

    // --- Quick Identity Switcher ---
    userPills.forEach(pill => {
        pill.addEventListener('click', (e) => {
            e.preventDefault();
            const uid = pill.dataset.user;
            userPills.forEach(p => p.classList.remove('active'));
            pill.classList.add('active');
            authenticateUser(uid);
        });
    });

    async function authenticateUser(userId) {
        try {
            const res = await fetch('/api/v1/auth/login', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ userId })
            });
            if (!res.ok) throw new Error('Auth failed');
            const data = await res.json();
            currentUser.userId = data.userId;
            currentUser.token = data.token;
            localStorage.setItem('slotguard_userId', currentUser.userId);
            localStorage.setItem('slotguard_token', currentUser.token);

            connectSSE();
            fetchSlots();
            showStatusBanner(`Authenticated as ${currentUser.userId}`, 'success');
        } catch (err) {
            console.error(err);
            showStatusBanner('Authentication error', 'error');
        }
    }

    // --- Health Check ---
    async function checkHealth() {
        try {
            const res = await fetch('/health');
            if (res.ok) {
                healthLabel.textContent = 'ONLINE';
            } else {
                healthLabel.textContent = 'DEGRADED';
            }
        } catch {
            healthLabel.textContent = 'OFFLINE';
        }
    }

    // --- Fetch & Render Pod Grid ---
    async function fetchSlots() {
        try {
            const res = await fetch('/api/v1/slots');
            if (!res.ok) return;
            const data = await res.json();
            slotsData = data.slots || [];
            renderPodsGrid();
            updateSelectedPodPanel();
        } catch (err) {
            console.error('Fetch error:', err);
        }
    }

    function renderPodsGrid() {
        podsGrid.innerHTML = '';
        slotsData.forEach(slot => {
            const pod = document.createElement('div');
            const isSelected = slot.id === selectedPodId;
            const isMine = slot.userId === currentUser.userId;

            let stateClass = 'state-avail';
            let tagText = 'Available';

            if (slot.status === 'CONFIRMED') {
                stateClass = 'state-conf';
                tagText = 'Booked';
            } else if (slot.status === 'HELD') {
                if (isMine) {
                    stateClass = 'state-mine';
                    tagText = 'Your Hold';
                } else {
                    stateClass = 'state-held';
                    tagText = 'Held';
                }
            }

            pod.className = `pod-card ${stateClass} ${isSelected ? 'selected' : ''}`;
            pod.innerHTML = `
                <div class="pod-num">#${slot.id}</div>
                <div class="pod-tag">${tagText}</div>
                ${slot.status === 'HELD' && slot.ttl ? `<div class="pod-timer-mini">${slot.ttl}s</div>` : ''}
            `;

            pod.addEventListener('click', () => {
                selectedPodId = slot.id;
                renderPodsGrid();
                updateSelectedPodPanel();
            });

            podsGrid.appendChild(pod);
        });
    }

    function updateSelectedPodPanel() {
        const slot = slotsData.find(s => s.id === selectedPodId) || { id: selectedPodId, status: 'AVAILABLE' };
        const isMine = slot.userId === currentUser.userId;

        selectedPodNum.textContent = `Pod #${slot.id}`;
        selectedPodBadge.textContent = slot.status;

        holdBtn.style.display = 'none';
        confirmBtn.style.display = 'none';
        waitlistBtn.style.display = 'none';
        ttlMetaRow.style.display = 'none';
        countdownRingBox.style.display = 'none';

        if (slot.status === 'CONFIRMED') {
            selectedPodBadge.className = 'pod-status-badge badge-conf';
            podHolderVal.textContent = slot.userId || 'Confirmed User';
        } else if (slot.status === 'HELD') {
            selectedPodBadge.className = isMine ? 'pod-status-badge badge-mine' : 'pod-status-badge badge-held';
            podHolderVal.textContent = isMine ? `${slot.userId} (You)` : (slot.userId || 'Another User');

            if (slot.ttl) {
                ttlMetaRow.style.display = 'flex';
                podTTLVal.textContent = `${slot.ttl}s`;

                if (isMine) {
                    countdownRingBox.style.display = 'block';
                    updateCircularTimer(slot.ttl);
                    confirmBtn.style.display = 'inline-flex';
                } else {
                    waitlistBtn.style.display = 'inline-flex';
                }
            }
        } else {
            selectedPodBadge.className = 'pod-status-badge badge-avail';
            podHolderVal.textContent = 'None';
            holdBtn.style.display = 'inline-flex';
        }

        podWaitlistVal.textContent = `${slot.waitlistCount || 0} Users`;
    }

    function updateCircularTimer(ttlSeconds) {
        const totalDuration = 60;
        const maxOffset = 264;
        const pct = Math.max(0, Math.min(1, ttlSeconds / totalDuration));
        const offset = maxOffset * (1 - pct);
        ringFillCircle.style.strokeDashoffset = offset;
        ringTimeText.textContent = `${ttlSeconds}s`;
    }

    // --- Action Handlers ---
    holdBtn.addEventListener('click', async (e) => {
        e.preventDefault();
        if (!currentUser.token) return alert('Please select a user identity');
        showStatusBanner('Executing atomic Redis hold script...', 'info');
        try {
            const res = await fetch(`/api/v1/slots/${selectedPodId}/hold`, {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${currentUser.token}`,
                    'Content-Type': 'application/json'
                }
            });
            const data = await res.json();
            if (res.ok) {
                showStatusBanner(data.message || `Pod #${selectedPodId} held successfully`, 'success');
                fetchSlots();
            } else {
                showStatusBanner(data.error || 'Failed to hold seat', 'error');
            }
        } catch {
            showStatusBanner('Network error', 'error');
        }
    });

    confirmBtn.addEventListener('click', async (e) => {
        e.preventDefault();
        if (!currentUser.token) return alert('Please select a user identity');
        showStatusBanner('Writing ACID transaction to PostgreSQL...', 'info');
        const idempotencyKey = `idemp_${Date.now()}_${Math.random()}`;
        try {
            const res = await fetch(`/api/v1/slots/${selectedPodId}/confirm`, {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${currentUser.token}`,
                    'Idempotency-Key': idempotencyKey,
                    'Content-Type': 'application/json'
                }
            });
            const data = await res.json();
            if (res.ok) {
                showStatusBanner(`Booking confirmed. ID: ${data.bookingId}`, 'success');
                fetchSlots();
            } else {
                showStatusBanner(data.error || 'Confirmation failed', 'error');
            }
        } catch {
            showStatusBanner('Network error', 'error');
        }
    });

    waitlistBtn.addEventListener('click', async (e) => {
        e.preventDefault();
        if (!currentUser.token) return alert('Please select a user identity');
        showStatusBanner('Adding user to Redis waitlist...', 'info');
        try {
            const res = await fetch(`/api/v1/slots/${selectedPodId}/waitlist`, {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${currentUser.token}`,
                    'Content-Type': 'application/json'
                }
            });
            const data = await res.json();
            if (res.ok) {
                showStatusBanner(data.message || 'Joined waitlist', 'success');
                fetchSlots();
            } else {
                showStatusBanner(data.error || 'Failed to join waitlist', 'error');
            }
        } catch {
            showStatusBanner('Network error', 'error');
        }
    });

    function showStatusBanner(msg, type) {
        actionStatusBanner.textContent = msg;
        actionStatusBanner.className = `status-banner ${type}`;
    }

    // --- Virtual Waiting Room Controls ---
    joinQueueBtn.addEventListener('click', async (e) => {
        e.preventDefault();
        if (!currentUser.token) return alert('Please select a user');
        try {
            const res = await fetch('/api/v1/queue/join', {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${currentUser.token}`,
                    'Content-Type': 'application/json'
                }
            });
            const data = await res.json();
            if (res.ok) {
                queueValText.textContent = 'Joined Queue';
                queueSubText.textContent = 'Waiting for background admission cycle...';
                checkQueueStatus();
            } else {
                alert(data.error || 'Failed to join queue');
            }
        } catch (err) {
            console.error(err);
        }
    });

    checkQueueBtn.addEventListener('click', (e) => {
        e.preventDefault();
        checkQueueStatus();
    });

    async function checkQueueStatus() {
        if (!currentUser.token) return;
        try {
            const res = await fetch('/api/v1/queue/status', {
                headers: { 'Authorization': `Bearer ${currentUser.token}` }
            });
            const data = await res.json();
            if (res.ok) {
                if (data.status === 'admitted') {
                    queueValText.textContent = 'ADMITTED';
                    queueSubText.textContent = 'Full access to core engine flows';
                    queueValText.style.color = 'var(--emerald-400)';
                } else {
                    queueValText.textContent = `Rank #${data.position}`;
                    queueSubText.textContent = 'Virtual waiting room queue position';
                }
            } else {
                queueValText.textContent = 'Not in Queue';
                queueSubText.textContent = data.error || 'Click Join Queue for admission';
            }
        } catch (err) {
            console.error(err);
        }
    }

    // --- SSE Event Stream ---
    function connectSSE() {
        if (eventSource) eventSource.close();
        if (!currentUser.token) return;

        eventSource = new EventSource(`/api/v1/queue/stream?token=${currentUser.token}`);

        eventSource.onopen = () => {
            sseLivePill.textContent = 'SSE STREAM';
            addSSELog('[SSE] Connected to event stream', 'sys');
        };

        eventSource.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                if (data.status === 'connected') {
                    addSSELog('[SSE] Stream active & ready', 'sys');
                } else if (data.status === 'admitted') {
                    addSSELog('[SSE Alert] User ADMITTED into system', 'event');
                    checkQueueStatus();
                } else if (data.status === 'seat_reassigned') {
                    addSSELog(`[SSE Reassign] Pod #${data.seatId} automatically reassigned to you.`, 'event');
                    fetchSlots();
                } else {
                    addSSELog(`[SSE Event] ${JSON.stringify(data)}`, 'event');
                }
            } catch {
                addSSELog(`[SSE] ${event.data}`, 'sys');
            }
        };

        eventSource.onerror = () => {
            sseLivePill.textContent = 'Reconnecting...';
        };
    }

    function addSSELog(msg, type) {
        const line = document.createElement('div');
        line.className = `term-line ${type}`;
        const time = new Date().toLocaleTimeString();
        line.textContent = `[${time}] ${msg}`;
        sseTerminal.appendChild(line);
        sseTerminal.scrollTop = sseTerminal.scrollHeight;
    }

    // --- 50-VU Lock Race Arena ---
    function initVUArenaGrid() {
        vuArenaGrid.innerHTML = '';
        for (let i = 1; i <= 50; i++) {
            const pod = document.createElement('div');
            pod.className = 'vu-pod';
            pod.id = `vu-pod-${i}`;
            pod.textContent = `VU #${i}`;
            vuArenaGrid.appendChild(pod);
        }
    }

    fireSimBtn.addEventListener('click', async (e) => {
        e.preventDefault();
        fireSimBtn.disabled = true;
        fireSimBtn.textContent = 'Running 50-VU Race...';
        initVUArenaGrid();
        
        simWinnerCount.textContent = '0';
        simConflictCount.textContent = '0';
        simLatencyVal.textContent = 'Measuring...';
        addSimLog('===== Launching 50 Concurrent Virtual Users for Seat #99 =====', 'sys');

        const targetSeatId = '99';
        const numVUs = 50;
        const promises = [];
        const startTime = performance.now();

        for (let i = 1; i <= numVUs; i++) {
            const vuUserId = `arena_vu_${i}`;
            promises.push((async () => {
                const authRes = await fetch('/api/v1/auth/login', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ userId: vuUserId })
                });
                const authData = await authRes.json();
                const reqStart = performance.now();

                const res = await fetch(`/api/v1/slots/${targetSeatId}/hold`, {
                    method: 'POST',
                    headers: {
                        'Authorization': `Bearer ${authData.token}`,
                        'Content-Type': 'application/json'
                    }
                });
                const latency = performance.now() - reqStart;
                return { vu: i, userId: vuUserId, status: res.status, latency };
            })());
        }

        const results = await Promise.all(promises);
        const wallTime = performance.now() - startTime;

        let winners = 0;
        let conflicts = 0;
        let winnerObj = null;

        results.forEach(r => {
            const pod = document.getElementById(`vu-pod-${r.vu}`);
            if (r.status === 200) {
                winners++;
                winnerObj = r;
                if (pod) pod.className = 'vu-pod winner';
                addSimLog(`[WINNER] VU #${r.vu} GRANTED LOCK in ${r.latency.toFixed(2)}ms`, 'event');
            } else if (r.status === 409) {
                conflicts++;
                if (pod) pod.className = 'vu-pod conflict';
            } else {
                addSimLog(`VU #${r.vu} returned status ${r.status}`, 'warn');
            }
        });

        simWinnerCount.textContent = winners;
        simConflictCount.textContent = conflicts;
        simPrecisionVal.textContent = winners === 1 ? '100% (Passed)' : 'Failed';
        simLatencyVal.textContent = winnerObj ? `${winnerObj.latency.toFixed(2)} ms` : 'N/A';

        fireSimBtn.disabled = false;
        fireSimBtn.innerHTML = `Fire 50-VU Concurrent Race`;

        addSimLog(`Race Completed in ${wallTime.toFixed(2)}ms wall time. Allowed: ${winners} | Rejected: ${conflicts}`, 'sys');
        fetchSlots();
    });

    function addSimLog(msg, type) {
        const line = document.createElement('div');
        line.className = `term-line ${type}`;
        line.textContent = msg;
        simTerminal.appendChild(line);
        simTerminal.scrollTop = simTerminal.scrollHeight;
    }

    // --- LangGraph AI Clinic Agent Chat Handlers ---
    promptChips.forEach(chip => {
        chip.addEventListener('click', (e) => {
            e.preventDefault();
            chatInput.value = chip.dataset.prompt;
            chatForm.dispatchEvent(new Event('submit'));
        });
    });

    chatForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        const text = chatInput.value.trim();
        if (!text) return;

        appendChatMessage('user', text);
        chatInput.value = '';

        const thinkingId = appendChatMessage('agent', '<em>Agent processing request via LangGraph...</em>');

        try {
            const res = await fetch('/api/v1/agent/chat', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    message: text,
                    userId: currentUser.userId
                })
            });

            const data = await res.json();
            const thinkingEl = document.getElementById(thinkingId);
            if (thinkingEl) thinkingEl.remove();

            if (res.ok) {
                let formattedReply = data.reply || 'No response from agent';

                let toolBadgeHtml = '';
                if (data.toolCalls && data.toolCalls.length > 0) {
                    const lastTool = data.toolCalls[data.toolCalls.length - 1];
                    toolBadgeHtml = `<div class="tool-badge">Tool Invoked: ${lastTool.name}(${JSON.stringify(lastTool.args)})</div>`;
                }

                appendChatMessage('agent', `${formattedReply} ${toolBadgeHtml}`);

                fetchSlots();
            } else {
                appendChatMessage('agent', `Error from Agent: ${data.error || 'Request failed'}`);
            }
        } catch (err) {
            console.error('Agent chat error:', err);
            appendChatMessage('agent', 'Network error connecting to LangGraph Agent.');
        }
    });

    function appendChatMessage(sender, htmlContent) {
        const msgId = `msg_${Date.now()}_${Math.random().toString(36).substr(2, 5)}`;
        const msgDiv = document.createElement('div');
        msgDiv.className = `chat-msg ${sender}-msg`;
        msgDiv.id = msgId;

        msgDiv.innerHTML = `<div class="msg-bubble">${htmlContent}</div>`;
        chatFeed.appendChild(msgDiv);
        chatFeed.scrollTop = chatFeed.scrollHeight;
        return msgId;
    }

    // --- Startup ---
    initVUArenaGrid();
    checkHealth();
    setInterval(checkHealth, 10000);

    if (userPills.length > 0) userPills[0].click();
    else authenticateUser(currentUser.userId);

    setInterval(fetchSlots, 3000);
});
