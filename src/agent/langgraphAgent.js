// langgraphAgent.js
// StateGraph-based AI Agent built with @langchain/langgraph & @langchain/core.

import { StateGraph, START, END, Annotation } from "@langchain/langgraph";
import { slotGuardTools } from "./slotGuardTools.js";

// 1. Define State Graph Schema using Annotation.Root
const AgentStateAnnotation = Annotation.Root({
    messages: Annotation({
        reducer: (current, update) => current.concat(update),
        default: () => []
    }),
    userId: Annotation({
        reducer: (current, update) => update || current,
        default: () => "user_1"
    }),
    toolCalls: Annotation({
        reducer: (current, update) => (update ? current.concat(update) : current),
        default: () => []
    }),
    finalReply: Annotation({
        reducer: (current, update) => update || current,
        default: () => ""
    })
});

// Create tool map for easy execution lookup
const toolMap = {};
slotGuardTools.forEach(t => {
    toolMap[t.name] = t;
});

/**
 * Node 1: Intent Analysis & Tool Decision Node
 */
async function intentAnalyzerNode(state) {
    const lastMsg = state.messages[state.messages.length - 1];
    const text = (typeof lastMsg === 'string' ? lastMsg : lastMsg?.content || '').toLowerCase();
    const userId = state.userId || 'user_1';

    const toolCallsToMake = [];

    // Extract slot numbers mentioned in text (e.g. "slot 5", "pod #12", "seat 3")
    const slotMatch = text.match(/(?:slot|pod|seat)\s*#?\s*(\d+)/i) || text.match(/\b(\d{1,2})\b/);
    const slotId = slotMatch ? slotMatch[1] : '1';

    if (text.includes('hold') || text.includes('reserve') || text.includes('lock')) {
        toolCallsToMake.push({
            name: 'hold_clinic_slot',
            args: { seatId: slotId, userId }
        });
    } else if (text.includes('confirm') || text.includes('book') || text.includes('pay') || text.includes('acid')) {
        toolCallsToMake.push({
            name: 'confirm_clinic_booking',
            args: { seatId: slotId, userId }
        });
    } else if (text.includes('waitlist') || text.includes('wait list') || text.includes('line')) {
        toolCallsToMake.push({
            name: 'join_slot_waitlist',
            args: { seatId: slotId, userId }
        });
    } else if (text.includes('available') || text.includes('status') || text.includes('list') || text.includes('show')) {
        toolCallsToMake.push({
            name: 'get_available_slots',
            args: {}
        });
    } else if (text.includes('queue') || text.includes('waiting room') || text.includes('admit')) {
        toolCallsToMake.push({
            name: 'join_waiting_room',
            args: { userId }
        });
    } else {
        toolCallsToMake.push({
            name: 'get_available_slots',
            args: {}
        });
    }

    return { toolCalls: toolCallsToMake };
}

/**
 * Node 2: Dynamic Tool Execution Node
 */
async function toolExecutionNode(state) {
    const executedResults = [];

    for (const call of state.toolCalls) {
        const tool = toolMap[call.name];
        if (tool) {
            try {
                const outputStr = await tool.invoke(call.args);
                const parsed = JSON.parse(outputStr);
                executedResults.push({
                    name: call.name,
                    args: call.args,
                    result: parsed
                });
            } catch (err) {
                executedResults.push({
                    name: call.name,
                    args: call.args,
                    result: { success: false, error: err.message }
                });
            }
        }
    }

    return { toolCalls: executedResults };
}

/**
 * Node 3: Conversational Response Synthesizer Node (Clean Markdown, Zero Emojis)
 */
async function responseSynthesizerNode(state) {
    const executed = state.toolCalls[state.toolCalls.length - 1];
    let reply = "";

    if (!executed) {
        reply = "SlotGuard AI Clinic Assistant active. You can request slot holds, booking confirmations, or waitlist status in natural language.";
    } else {
        const { name, args, result } = executed;
        if (name === 'hold_clinic_slot') {
            if (result.success) {
                reply = `**Lock Granted:** Atomic Redis Lua lock executed for Slot #${args.seatId} (User: ${args.userId}). Duration: 60 seconds.`;
            } else {
                reply = `**Hold Conflict:** ${result.error}`;
            }
        } else if (name === 'confirm_clinic_booking') {
            if (result.success) {
                reply = `**Booking Confirmed:** Slot #${args.seatId} committed to PostgreSQL database for ${args.userId}. Booking ID: #${result.bookingId}.`;
            } else {
                reply = `**Confirmation Error:** ${result.error}`;
            }
        } else if (name === 'join_slot_waitlist') {
            if (result.success) {
                reply = `**Waitlist Registered:** User ${args.userId} placed on waitlist for Slot #${args.seatId} at rank #${result.position}.`;
            } else {
                reply = `**Waitlist Notice:** ${result.error}`;
            }
        } else if (name === 'get_available_slots') {
            if (result.success) {
                const avail = result.slots.filter(s => s.status === 'AVAILABLE').length;
                const held = result.slots.filter(s => s.status === 'HELD').length;
                const conf = result.slots.filter(s => s.status === 'CONFIRMED').length;
                reply = `**Slot Status Summary:** ${avail} Available, ${held} Held, ${conf} Confirmed. Total slots: 24.`;
            } else {
                reply = `Status query failed: ${result.error}`;
            }
        } else if (name === 'join_waiting_room') {
            reply = `**Admission Queue:** ${result.message}`;
        }
    }

    return { finalReply: reply };
}

// 2. Build LangGraph Workflow State Graph
const workflow = new StateGraph(AgentStateAnnotation)
    .addNode("analyzer", intentAnalyzerNode)
    .addNode("executor", toolExecutionNode)
    .addNode("synthesizer", responseSynthesizerNode)
    .addEdge(START, "analyzer")
    .addEdge("analyzer", "executor")
    .addEdge("executor", "synthesizer")
    .addEdge("synthesizer", END);

// 3. Compile LangGraph State Graph
const compiledGraph = workflow.compile();

/**
 * Main Agent Entrypoint
 */
export async function runClinicAgent(userMessage, userId = 'user_1') {
    const initialState = {
        messages: [{ role: 'user', content: userMessage }],
        userId: userId,
        toolCalls: [],
        finalReply: ""
    };

    const finalResult = await compiledGraph.invoke(initialState);
    return {
        reply: finalResult.finalReply,
        toolCalls: finalResult.toolCalls,
        userId: finalResult.userId
    };
}
