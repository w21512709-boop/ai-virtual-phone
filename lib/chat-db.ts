// lib/chat-db.ts
// IndexedDB persistence layer for chat data using Dexie.js.
// Provides async persistence behind the synchronous in-memory cache in chat-storage.ts.

import Dexie from "dexie";
import type { ChatMessage, ChatSession, ChatContact } from "./chat-storage";

// ── Database Schema ──────────────────────────────

class ChatDatabase extends Dexie {
    messages!: Dexie.Table<ChatMessage, string>;
    sessions!: Dexie.Table<ChatSession, string>;
    contacts!: Dexie.Table<ChatContact, string>;

    constructor() {
        super("AiPhoneChatDB");
        this.version(1).stores({
            messages: "id, sessionId, createdAt",
            sessions: "id, contactId",
            contacts: "id, characterId",
        });
    }
}

export const chatDb = new ChatDatabase();

// ── Initialization + Migration from localStorage ──

const LS_MESSAGES_KEY = "ai_phone_chat_messages_v1";
const LS_SESSIONS_KEY = "ai_phone_chat_sessions_v1";
const LS_CONTACTS_KEY = "ai_phone_chat_contacts_v1";
const LS_MIGRATED_FLAG = "ai_phone_idb_migrated_v1";

/**
 * Initialize IndexedDB and migrate data from localStorage if needed.
 * Returns the loaded data for the in-memory caches.
 */
export async function initChatDb(): Promise<{
    messages: ChatMessage[];
    sessions: ChatSession[];
    contacts: ChatContact[];
}> {
    if (typeof window === "undefined") {
        return { messages: [], sessions: [], contacts: [] };
    }

    const alreadyMigrated = window.localStorage.getItem(LS_MIGRATED_FLAG);

    if (!alreadyMigrated) {
        // Guard against a lost migration flag while IndexedDB still holds data.
        // The flag lives in volatile localStorage; the actual data lives in the far
        // more durable IndexedDB. The flag can disappear independently of the data
        // (e.g. clearing the "cache" data module, privacy tooling, partial eviction).
        // If we blindly "re-migrated" from now-empty localStorage we would shadow
        // real data with empty caches, and the next write would wipe IndexedDB.
        // So when IDB already has data, treat it as already migrated and reuse it.
        try {
            const existingCount =
                (await chatDb.messages.count()) +
                (await chatDb.sessions.count()) +
                (await chatDb.contacts.count());
            if (existingCount > 0) {
                window.localStorage.setItem(LS_MIGRATED_FLAG, "1");
                const [messages, sessions, contacts] = await Promise.all([
                    chatDb.messages.toArray(),
                    chatDb.sessions.toArray(),
                    chatDb.contacts.toArray(),
                ]);
                console.log(`[ChatDB] Migration flag missing but IndexedDB has data; reusing it: ${messages.length} messages, ${sessions.length} sessions, ${contacts.length} contacts`);
                return { messages, sessions, contacts };
            }
        } catch (err) {
            console.warn("[ChatDB] Pre-migration IndexedDB check failed:", err);
        }

        // First run after migration: move localStorage data → IndexedDB
        try {
            const rawMessages = window.localStorage.getItem(LS_MESSAGES_KEY);
            const rawSessions = window.localStorage.getItem(LS_SESSIONS_KEY);
            const rawContacts = window.localStorage.getItem(LS_CONTACTS_KEY);

            const lsMessages: ChatMessage[] = rawMessages ? JSON.parse(rawMessages) : [];
            const lsSessions: ChatSession[] = rawSessions ? JSON.parse(rawSessions) : [];
            const lsContacts: ChatContact[] = rawContacts ? JSON.parse(rawContacts) : [];

            if (lsMessages.length > 0) {
                await chatDb.messages.bulkPut(lsMessages);
            }
            if (lsSessions.length > 0) {
                await chatDb.sessions.bulkPut(lsSessions);
            }
            if (lsContacts.length > 0) {
                await chatDb.contacts.bulkPut(lsContacts);
            }

            // Mark as migrated and remove old localStorage data
            window.localStorage.setItem(LS_MIGRATED_FLAG, "1");
            window.localStorage.removeItem(LS_MESSAGES_KEY);
            window.localStorage.removeItem(LS_SESSIONS_KEY);
            window.localStorage.removeItem(LS_CONTACTS_KEY);

            console.log(`[ChatDB] Migrated from localStorage: ${lsMessages.length} messages, ${lsSessions.length} sessions, ${lsContacts.length} contacts`);

            return { messages: lsMessages, sessions: lsSessions, contacts: lsContacts };
        } catch (err) {
            console.error("[ChatDB] Migration failed, falling back to localStorage:", err);
            // If migration fails, load from localStorage as fallback
            const fallbackMessages: ChatMessage[] = safeParse(window.localStorage.getItem(LS_MESSAGES_KEY));
            const fallbackSessions: ChatSession[] = safeParse(window.localStorage.getItem(LS_SESSIONS_KEY));
            const fallbackContacts: ChatContact[] = safeParse(window.localStorage.getItem(LS_CONTACTS_KEY));
            return { messages: fallbackMessages, sessions: fallbackSessions, contacts: fallbackContacts };
        }
    }

    // Already migrated: load from IndexedDB (retry up to 3 times on failure)
    let lastErr: unknown;
    for (let attempt = 0; attempt < 3; attempt++) {
        try {
            const [messages, sessions, contacts] = await Promise.all([
                chatDb.messages.toArray(),
                chatDb.sessions.toArray(),
                chatDb.contacts.toArray(),
            ]);
            console.log(`[ChatDB] Loaded from IndexedDB: ${messages.length} messages, ${sessions.length} sessions, ${contacts.length} contacts`);
            return { messages, sessions, contacts };
        } catch (err) {
            lastErr = err;
            console.warn(`[ChatDB] Load attempt ${attempt + 1}/3 failed:`, err);
            if (attempt < 2) {
                await new Promise(r => setTimeout(r, 300 * (attempt + 1)));
                try { if (!chatDb.isOpen()) await chatDb.open(); } catch {}
            }
        }
    }
    console.error("[ChatDB] All load attempts failed:", lastErr);
    throw new Error("[ChatDB] Failed to load after 3 attempts");
}

function safeParse<T>(raw: string | null): T[] {
    try { return raw ? JSON.parse(raw) : []; } catch { return []; }
}

// ── Async persistence helpers ──
//
// ⚠️ iOS 兼容性说明（针对 iOS Safari 闪退）：
// 之前这里所有写库都是 fire-and-forget，写失败只在控制台打一条 warn，界面毫无感知。
// 在 iOS 上，IndexedDB 写入可能在低内存/存储压力下失败或被系统打断：消息看着已经发出去了，
// 但实际没写进数据库。页面一旦被系统杀掉重载，就只剩写成功的那一部分 —— 这就是「消息只剩一半」。
// 因此这里统一改成：失败先重试一次（短暂延迟），仍失败则派发事件让界面提示用户，不再静默吞掉。

/** 触发「本地数据库写入失败」事件，供界面提示（见 components/main-app.tsx 的监听）。 */
export function notifyDbWriteFailure(context: string, err: unknown): void {
  if (typeof window === "undefined") return;
  try {
    const detail = {
      context,
      message: err instanceof Error ? err.message : String(err),
    };
    window.dispatchEvent(new CustomEvent("chat-db-write-failure", { detail }));
  } catch {
    /* 提示失败不影响主流程 */
  }
}

/**
 * 对一次写库操作做「重试一次 + 失败通知」。
 * 保持原 fire-and-forget 风格：返回 Promise<void>，调用方无需 await。
 */
async function persistWithRetry(context: string, op: () => Promise<unknown>): Promise<void> {
  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      await op();
      return;
    } catch (err) {
      if (attempt === 0) {
        // 首次失败：等 400ms 重试一次（iOS 上瞬时失败常见，重试往往能成功）
        await new Promise((resolve) => setTimeout(resolve, 400));
        continue;
      }
      console.error(`[ChatDB] ${context} 写入失败（已重试一次）:`, err);
      notifyDbWriteFailure(context, err);
    }
  }
}

function putMessageWithRetry(msg: ChatMessage): void {
  void persistWithRetry("put message", () => chatDb.messages.put(msg));
}

function putMessagesWithRetry(msgs: ChatMessage[]): void {
  if (msgs.length === 0) return;
  void persistWithRetry(`bulkPut ${msgs.length} messages`, () => chatDb.messages.bulkPut(msgs));
}

export function dbPutMessage(msg: ChatMessage): void {
  putMessageWithRetry(msg);
}

export function dbPutMessages(msgs: ChatMessage[]): void {
  putMessagesWithRetry(msgs);
}

export function dbDeleteMessage(id: string): void {
  void persistWithRetry("delete message", () => chatDb.messages.delete(id));
}

export function dbDeleteMessagesBySession(sessionId: string): void {
  void persistWithRetry(
    "delete session messages",
    () => chatDb.messages.where("sessionId").equals(sessionId).delete(),
  );
}

export function dbDeleteMessagesByIds(ids: string[]): void {
  if (ids.length === 0) return;
  void persistWithRetry(`bulkDelete ${ids.length} messages`, () => chatDb.messages.bulkDelete(ids));
}

export function dbPutSession(session: ChatSession): void {
  void persistWithRetry("put session", () => chatDb.sessions.put(session));
}

export function dbPutSessions(sessions: ChatSession[]): void {
  if (sessions.length === 0) return;
  void persistWithRetry(`bulkPut ${sessions.length} sessions`, () => chatDb.sessions.bulkPut(sessions));
}

export function dbReplaceSessions(sessions: ChatSession[]): void {
  void persistWithRetry("replace sessions", async () => {
    await chatDb.transaction("rw", chatDb.sessions, async () => {
      await chatDb.sessions.clear();
      await chatDb.sessions.bulkPut(sessions);
    });
  });
}

export function dbDeleteSession(id: string): void {
  void persistWithRetry("delete session", () => chatDb.sessions.delete(id));
}

export function dbPutContacts(contacts: ChatContact[]): void {
  if (contacts.length === 0) return;
  void persistWithRetry(`bulkPut ${contacts.length} contacts`, () => chatDb.contacts.bulkPut(contacts));
}

export function dbClearContacts(): void {
  void persistWithRetry("clear contacts", () => chatDb.contacts.clear());
}

export function dbReplaceContacts(contacts: ChatContact[]): void {
  void persistWithRetry("replaceContacts", async () => {
    await chatDb.transaction("rw", chatDb.contacts, async () => {
      await chatDb.contacts.clear();
      await chatDb.contacts.bulkPut(contacts);
    });
  });
}
