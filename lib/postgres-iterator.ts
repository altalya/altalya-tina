/**
 * PostgreSQL Iterator for abstract-level
 * Implements range queries with lexicographic ordering
 */
import { AbstractIterator, AbstractLevel } from 'abstract-level';
import { NextCallback } from 'abstract-level/types/abstract-iterator';
import { Pool } from 'pg';

const DEFAULT_LIMIT = 50;

export interface IteratorOptions<KDefault> {
    offset: number;
    limit: number;
    keyEncoding: string;
    valueEncoding: string;
    reverse: boolean;
    keys: boolean;
    values: boolean;
    gt?: KDefault;
    gte?: KDefault;
    lt?: KDefault;
    lte?: KDefault;
    debug: boolean;
}

// Interface for the database instance to avoid circular dependency
export interface PostgresLevelLike<KDefault, VDefault> extends AbstractLevel<string, KDefault, VDefault> {
    pool: Pool;
    namespace: string;
}

export class PostgresIterator<KDefault, VDefault> extends AbstractIterator<
    PostgresLevelLike<KDefault, VDefault>,
    KDefault,
    VDefault
> {
    private pool: Pool;
    private namespace: string;
    private options: IteratorOptions<KDefault>;
    private offset: number;
    private readonly resultLimit: number;
    private results: Array<[KDefault | undefined, VDefault | undefined]>;
    private finished: boolean;
    private debug: boolean;

    constructor(
        db: PostgresLevelLike<KDefault, VDefault>,
        options: IteratorOptions<KDefault>
    ) {
        super(db, options);
        this.pool = db.pool;
        this.namespace = db.namespace;
        this.options = options;
        this.resultLimit =
            options.limit !== Infinity && options.limit >= 0
                ? options.limit
                : DEFAULT_LIMIT;
        this.offset = options.offset || 0;
        this.results = [];
        this.finished = false;
        this.debug = options.debug || false;
    }

    async _next(callback: NextCallback<KDefault, VDefault>) {
        if (this.finished) {
            return this.db.nextTick(callback, null);
        }

        if (this.results.length === 0) {
            const getKeys = this.options.keys;
            const getValues = this.options.values;

            // Build WHERE conditions for range queries
            const conditions: string[] = ['namespace = $1'];
            const params: (string | number)[] = [this.namespace];
            let paramIndex = 2;

            if (this.options.gt !== undefined) {
                conditions.push(`key > $${paramIndex}`);
                params.push(String(this.options.gt));
                paramIndex++;
            } else if (this.options.gte !== undefined) {
                conditions.push(`key >= $${paramIndex}`);
                params.push(String(this.options.gte));
                paramIndex++;
            }

            if (this.options.lt !== undefined) {
                conditions.push(`key < $${paramIndex}`);
                params.push(String(this.options.lt));
                paramIndex++;
            } else if (this.options.lte !== undefined) {
                conditions.push(`key <= $${paramIndex}`);
                params.push(String(this.options.lte));
                paramIndex++;
            }

            const orderDirection = this.options.reverse ? 'DESC' : 'ASC';
            const selectColumns: string[] = [];
            if (getKeys) selectColumns.push('key');
            if (getValues) selectColumns.push('value');
            if (selectColumns.length === 0) selectColumns.push('key');

            const query = `
        SELECT ${selectColumns.join(', ')}
        FROM tina_kv
        WHERE ${conditions.join(' AND ')}
        ORDER BY key ${orderDirection}
        LIMIT $${paramIndex}
        OFFSET $${paramIndex + 1}
      `;

            params.push(this.resultLimit, this.offset);

            if (this.debug) {
                console.log('PostgresIterator query:', query, params);
            }

            try {
                const result = await this.pool.query(query, params);

                if (!result.rows || result.rows.length === 0) {
                    this.finished = true;
                    return this.db.nextTick(callback, null);
                }

                for (const row of result.rows) {
                    const resultItem: [KDefault | undefined, VDefault | undefined] = [
                        getKeys ? (row.key as KDefault) : undefined,
                        getValues ? (row.value as VDefault) : undefined,
                    ];
                    this.results.push(resultItem);
                }

                this.offset += this.resultLimit;
            } catch (e) {
                console.error('PostgresIterator error:', e);
                this.finished = true;
                return this.db.nextTick(callback, null);
            }
        }

        const result = this.results.shift();
        if (this.debug) {
            console.log('PostgresIterator result:', result);
        }

        if (result) {
            return this.db.nextTick(callback, null, result[0], result[1]);
        }

        return this.db.nextTick(callback, null);
    }
}
