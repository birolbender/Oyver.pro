import { pgTable, serial, bigserial, uuid, varchar, text, boolean, integer, timestamp, numeric } from 'drizzle-orm/pg-core';

export const users = pgTable('users', {
  id: uuid('id').defaultRandom().primaryKey(),
  username: varchar('username', { length: 32 }).notNull().unique(),
  phoneNumber: varchar('phone_number', { length: 20 }).unique(),
  isPhoneVerified: boolean('is_phone_verified').default(false).notNull(),

  tckn: varchar('tckn', { length: 11 }).unique(),
  legalFirstName: varchar('legal_first_name', { length: 64 }),
  legalLastName: varchar('legal_last_name', { length: 64 }),
  birthYear: integer('birth_year'),
  iban: varchar('iban', { length: 34 }),
  kycStatus: varchar('kyc_status', { length: 16 }).default('UNVERIFIED').notNull(),

  balanceVeraWithdrawable: numeric('balance_vera_withdrawable', { precision: 18, scale: 4 }).default('0.0000').notNull(),
  balanceVeraPromo: numeric('balance_vera_promo', { precision: 18, scale: 4 }).default('500.0000').notNull(),

  currentStreak: integer('current_streak').default(1).notNull(),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull()
});

export const veraTransactions = pgTable('vera_transactions', {
  id: bigserial('id', { mode: 'number' }).primaryKey(),
  userId: uuid('user_id').references(() => users.id, { onDelete: 'cascade' }).notNull(),
  accountType: varchar('account_type', { length: 16 }).notNull(),
  type: varchar('type', { length: 32 }).notNull(),
  amount: numeric('amount', { precision: 18, scale: 4 }).notNull(),
  taxWithheldVera: numeric('tax_withheld_vera', { precision: 18, scale: 4 }).default('0.0000').notNull(),
  balanceAfter: numeric('balance_after', { precision: 18, scale: 4 }).notNull(),
  referenceId: varchar('reference_id', { length: 64 }),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull()
});

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

  status: varchar('status', { length: 32 }).default('TRADING').notNull(),
  winningOutcome: varchar('winning_outcome', { length: 8 }),

  poolYes: numeric('pool_yes', { precision: 18, scale: 4 }).default('10000.0000').notNull(),
  poolNo: numeric('pool_no', { precision: 18, scale: 4 }).default('10000.0000').notNull(),
  volumeVera: numeric('volume_vera', { precision: 18, scale: 4 }).default('0.0000').notNull(),
  totalPredictionsCount: integer('total_predictions_count').default(0).notNull(),

  isHero: boolean('is_hero').default(false).notNull(),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull()
});

export const positions = pgTable('positions', {
  id: bigserial('id', { mode: 'number' }).primaryKey(),
  userId: uuid('user_id').references(() => users.id, { onDelete: 'cascade' }).notNull(),
  marketId: integer('market_id').references(() => markets.id, { onDelete: 'cascade' }).notNull(),
  outcome: varchar('outcome', { length: 8 }).notNull(),
  sharesCount: numeric('shares_count', { precision: 18, scale: 4 }).notNull(),
  totalCostVera: numeric('total_cost_vera', { precision: 18, scale: 4 }).notNull(),
  isSettled: boolean('is_settled').default(false).notNull(),
  isClosedEarly: boolean('is_closed_early').default(false).notNull(),
  cashoutReturnVera: numeric('cashout_return_vera', { precision: 18, scale: 4 }),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).defaultNow().notNull()
});
