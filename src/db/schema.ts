import { pgTable, serial, bigserial, uuid, varchar, text, boolean, integer, timestamp, numeric, uniqueIndex } from 'drizzle-orm/pg-core';

// 1. KULLANICILAR (TCKN/IBAN YOK - TAMAMEN SANAL PROFİL)
export const users = pgTable('users', {
  id: uuid('id').defaultRandom().primaryKey(),
  sessionToken: varchar('session_token', { length: 64 }).notNull().unique(),
  username: varchar('username', { length: 32 }).notNull().unique(),
  phoneNumber: varchar('phone_number', { length: 20 }),
  isPhoneVerified: boolean('is_phone_verified').default(false).notNull(),

  // Tekil Bakiye: Yalnızca Kapalı Devre KOR Puanı
  balanceKor: numeric('balance_kor', { precision: 18, scale: 4 }).default('1000.0000').notNull(),

  // Yetenek ve Seri Metrikleri
  ratingEdge: integer('rating_edge').default(500).notNull(), // 0 - 1000 Puan
  currentStreak: integer('current_streak').default(1).notNull(),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull()
});

// 2. ÇİFT KAYITLI DEFTER: HESAPLAR (SIFIR TOPLAMLI MUHASEBE)
export const accounts = pgTable('accounts', {
  id: bigserial('id', { mode: 'number' }).primaryKey(),
  userId: uuid('user_id').references(() => users.id, { onDelete: 'cascade' }),
  accountType: varchar('account_type', { length: 32 }).notNull(), // USER_WALLET, MARKET_POOL_YES, MARKET_POOL_NO, TREASURY_BURN
  balance: numeric('balance', { precision: 18, scale: 4 }).default('0.0000').notNull(),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull()
});

// 3. DEFTER YEVMİYE KAYITLARI
export const journalEntries = pgTable('journal_entries', {
  id: bigserial('id', { mode: 'number' }).primaryKey(),
  referenceType: varchar('reference_type', { length: 32 }).notNull(), // INITIAL_GRANT, TRADE_BUY, TRADE_CASHOUT, DAILY_BONUS, FEE_BURN
  referenceId: varchar('reference_id', { length: 64 }).notNull(),
  description: text('description').notNull(),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull()
});

export const journalLines = pgTable('journal_lines', {
  id: bigserial('id', { mode: 'number' }).primaryKey(),
  entryId: bigserial('entry_id', { mode: 'number' }).references(() => journalEntries.id, { onDelete: 'cascade' }).notNull(),
  accountId: bigserial('account_id', { mode: 'number' }).references(() => accounts.id).notNull(),
  debit: numeric('debit', { precision: 18, scale: 4 }).default('0.0000').notNull(),
  credit: numeric('credit', { precision: 18, scale: 4 }).default('0.0000').notNull()
});

// 4. PAZARLAR
export const markets = pgTable('markets', {
  id: serial('id').primaryKey(),
  slug: varchar('slug', { length: 128 }).notNull().unique(),
  title: varchar('title', { length: 255 }).notNull(),
  category: varchar('category', { length: 64 }).notNull(),
  rules: text('rules').notNull(),
  sourceName: varchar('source_name', { length: 128 }).notNull(),
  sourceUrl: varchar('source_url', { length: 512 }),

  startsAt: timestamp('starts_at', { withTimezone: true }).defaultNow().notNull(),
  closesAt: timestamp('closes_at', { withTimezone: true }).notNull(),
  resolvedAt: timestamp('resolved_at', { withTimezone: true }),

  status: varchar('status', { length: 32 }).default('TRADING').notNull(), // TRADING, PENDING_RESOLUTION, RESOLVED, VOID
  winningOutcome: varchar('winning_outcome', { length: 8 }),

  poolYes: numeric('pool_yes', { precision: 18, scale: 4 }).default('10000.0000').notNull(),
  poolNo: numeric('pool_no', { precision: 18, scale: 4 }).default('10000.0000').notNull(),
  volumeKor: numeric('volume_kor', { precision: 18, scale: 4 }).default('0.0000').notNull(),
  totalPredictionsCount: integer('total_predictions_count').default(0).notNull(),

  isHero: boolean('is_hero').default(false).notNull(),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull()
});

// 5. POZİSYONLAR (KÂR AL / ERKEN SATIŞ DESTEKLİ)
export const positions = pgTable('positions', {
  id: bigserial('id', { mode: 'number' }).primaryKey(),
  userId: uuid('user_id').references(() => users.id, { onDelete: 'cascade' }).notNull(),
  marketId: integer('market_id').references(() => markets.id, { onDelete: 'cascade' }).notNull(),
  outcome: varchar('outcome', { length: 8 }).notNull(), // YES, NO
  sharesCount: numeric('shares_count', { precision: 18, scale: 4 }).notNull(),
  totalCostKor: numeric('total_cost_kor', { precision: 18, scale: 4 }).notNull(),
  isSettled: boolean('is_settled').default(false).notNull(),
  isClosedEarly: boolean('is_closed_early').default(false).notNull(),
  cashoutReturnKor: numeric('cashout_return_kor', { precision: 18, scale: 4 }),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).defaultNow().notNull()
});

// 6. GÜNLÜK SORU CEVAPLARI (A1 EXPLOIT ENGELİ: 1 KULLANICI GÜNDE 1 CEVAP)
export const dailyAnswers = pgTable('daily_answers', {
  id: bigserial('id', { mode: 'number' }).primaryKey(),
  userId: uuid('user_id').references(() => users.id, { onDelete: 'cascade' }).notNull(),
  questionId: integer('question_id').notNull(),
  answerDate: varchar('answer_date', { length: 10 }).notNull(), // YYYY-MM-DD
  outcome: varchar('outcome', { length: 8 }).notNull(),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull()
}, (table) => ({
  userDailyUnq: uniqueIndex('user_daily_q_unq').on(table.userId, table.questionId, table.answerDate)
}));
