/**
 * PostgreSQL Level Database Adapter for TinaCMS
 * Implements abstract-level interface using PostgreSQL as the backing store
 */
import {
    AbstractLevel,
    AbstractDatabaseOptions,
    AbstractOpenOptions,
} from 'abstract-level';
import { Pool, PoolConfig } from 'pg';
import { PostgresIterator, IteratorOptions } from './postgres-iterator';

// Custom error class compatible with abstract-level expectations
class LevelError extends Error {
    code: string;
    constructor(message: string, options: { code: string }) {
        super(message);
        this.code = options.code;
        this.name = 'LevelError';
    }
}


export interface PostgresLevelOptions<K, V> extends AbstractDatabaseOptions<K, V> {
    connectionString?: string;
    poolConfig?: PoolConfig;
    namespace?: string;
    debug?: boolean;
}

interface BatchOperation {
    type: 'put' | 'del';
    key: string;
    value?: string;
}

interface ClearOptions<KDefault> {
    gt?: KDefault;
    gte?: KDefault;
    lt?: KDefault;
    lte?: KDefault;
    limit: number;
    reverse: boolean;
    keyEncoding: string;
    valueEncoding: string;
}

export class PostgresLevel<
    KDefault = string,
    VDefault = string
> extends AbstractLevel<string, KDefault, VDefault> {
    public readonly pool: Pool;
    public readonly namespace: string;
    private readonly debug: boolean;
    private initialized: boolean = false;

    constructor(options: PostgresLevelOptions<KDefault, VDefault>) {
        super({ encodings: { utf8: true }, snapshots: false }, options);

        const poolConfig: PoolConfig = options.poolConfig || {
            connectionString: options.connectionString,
        };

        this.pool = new Pool(poolConfig);
        this.namespace = options.namespace || 'level';
        this.debug = options.debug || false;
    }

    get type() {
        return 'postgres';
    }

    private async ensureTable(): Promise<void> {
        if (this.initialized) return;

        const createTableQuery = `
      CREATE TABLE IF NOT EXISTS tina_kv (
        namespace TEXT NOT NULL,
        key TEXT NOT NULL,
        value TEXT NOT NULL,
        PRIMARY KEY (namespace, key)
      );
    `;

        const createIndexQuery = `
      CREATE INDEX IF NOT EXISTS idx_tina_kv_namespace_key
      ON tina_kv (namespace, key);
    `;

        try {
            await this.pool.query(createTableQuery);
            await this.pool.query(createIndexQuery);
            this.initialized = true;
            if (this.debug) {
                console.log('PostgresLevel: Table and index created/verified');
            }
        } catch (e) {
            console.error('PostgresLevel: Failed to create table:', e);
            throw e;
        }
    }

    async _open(
        options: AbstractOpenOptions,
        callback: (error?: Error) => void
    ): Promise<void> {
        if (this.debug) {
            console.log('PostgresLevel#_open');
        }

        try {
            await this.ensureTable();
            this.nextTick(callback);
        } catch (e) {
            this.nextTick(callback, e as Error);
        }
    }

    async _close(callback: (error?: Error) => void): Promise<void> {
        if (this.debug) {
            console.log('PostgresLevel#_close');
        }

        try {
            await this.pool.end();
            this.nextTick(callback);
        } catch (e) {
            this.nextTick(callback, e as Error);
        }
    }

    async _get(
        key: string,
        options: { keyEncoding: 'utf8'; valueEncoding: 'utf8' },
        callback: (error?: Error, value?: string) => void
    ): Promise<void> {
        if (this.debug) {
            console.log('PostgresLevel#_get', key);
        }

        try {
            const result = await this.pool.query(
                'SELECT value FROM tina_kv WHERE namespace = $1 AND key = $2',
                [this.namespace, key]
            );

            if (result.rows.length > 0) {
                if (this.debug) {
                    console.log('PostgresLevel#_get found:', result.rows[0].value);
                }
                return this.nextTick(callback, null, result.rows[0].value);
            } else {
                return this.nextTick(
                    callback,
                    new LevelError(`Key '${key}' was not found`, {
                        code: 'LEVEL_NOT_FOUND',
                    })
                );
            }
        } catch (e) {
            return this.nextTick(callback, e as Error);
        }
    }

    async _getMany(
        keys: string[],
        options: { keyEncoding: 'utf8'; valueEncoding: 'utf8' },
        callback: (error?: Error, values?: (string | undefined)[]) => void
    ): Promise<void> {
        if (this.debug) {
            console.log('PostgresLevel#_getMany', keys);
        }

        try {
            if (keys.length === 0) {
                return this.nextTick(callback, null, []);
            }

            const placeholders = keys.map((_, i) => `$${i + 2}`).join(', ');
            const result = await this.pool.query(
                `SELECT key, value FROM tina_kv WHERE namespace = $1 AND key IN (${placeholders})`,
                [this.namespace, ...keys]
            );

            const valueMap = new Map<string, string>();
            for (const row of result.rows) {
                valueMap.set(row.key, row.value);
            }

            const values = keys.map((key) => valueMap.get(key));
            return this.nextTick(callback, null, values);
        } catch (e) {
            return this.nextTick(callback, e as Error);
        }
    }

    async _put(
        key: string,
        value: string,
        options: { keyEncoding: 'utf8'; valueEncoding: 'utf8' },
        callback: (error?: Error) => void
    ): Promise<void> {
        if (this.debug) {
            console.log('PostgresLevel#_put', key, value);
        }

        try {
            await this.pool.query(
                `INSERT INTO tina_kv (namespace, key, value)
         VALUES ($1, $2, $3)
         ON CONFLICT (namespace, key)
         DO UPDATE SET value = EXCLUDED.value`,
                [this.namespace, key, value]
            );
            this.nextTick(callback);
        } catch (e) {
            this.nextTick(callback, e as Error);
        }
    }

    async _del(
        key: string,
        options: { keyEncoding: 'utf8' },
        callback: (error?: Error) => void
    ): Promise<void> {
        if (this.debug) {
            console.log('PostgresLevel#_del', key);
        }

        try {
            await this.pool.query(
                'DELETE FROM tina_kv WHERE namespace = $1 AND key = $2',
                [this.namespace, key]
            );
            this.nextTick(callback);
        } catch (e) {
            this.nextTick(callback, e as Error);
        }
    }

    async _batch(
        operations: BatchOperation[],
        options: object,
        callback: (error?: Error) => void
    ): Promise<void> {
        if (this.debug) {
            console.log('PostgresLevel#_batch', operations);
        }

        if (operations.length === 0) {
            return this.nextTick(callback);
        }

        const client = await this.pool.connect();

        try {
            await client.query('BEGIN');

            for (const op of operations) {
                if (op.type === 'put') {
                    await client.query(
                        `INSERT INTO tina_kv (namespace, key, value)
             VALUES ($1, $2, $3)
             ON CONFLICT (namespace, key)
             DO UPDATE SET value = EXCLUDED.value`,
                        [this.namespace, op.key, op.value]
                    );
                } else if (op.type === 'del') {
                    await client.query(
                        'DELETE FROM tina_kv WHERE namespace = $1 AND key = $2',
                        [this.namespace, op.key]
                    );
                }
            }

            await client.query('COMMIT');
            this.nextTick(callback);
        } catch (e) {
            await client.query('ROLLBACK');
            this.nextTick(callback, e as Error);
        } finally {
            client.release();
        }
    }

    async _clear(
        options: ClearOptions<KDefault>,
        callback: (error?: Error) => void
    ): Promise<void> {
        if (this.debug) {
            console.log('PostgresLevel#_clear', options);
        }

        try {
            const conditions: string[] = ['namespace = $1'];
            const params: (string | number)[] = [this.namespace];
            let paramIndex = 2;

            if (options.gt !== undefined) {
                conditions.push(`key > $${paramIndex}`);
                params.push(String(options.gt));
                paramIndex++;
            } else if (options.gte !== undefined) {
                conditions.push(`key >= $${paramIndex}`);
                params.push(String(options.gte));
                paramIndex++;
            }

            if (options.lt !== undefined) {
                conditions.push(`key < $${paramIndex}`);
                params.push(String(options.lt));
                paramIndex++;
            } else if (options.lte !== undefined) {
                conditions.push(`key <= $${paramIndex}`);
                params.push(String(options.lte));
                paramIndex++;
            }

            let query = `DELETE FROM tina_kv WHERE ${conditions.join(' AND ')}`;

            if (options.limit !== Infinity && options.limit >= 0) {
                const orderDirection = options.reverse ? 'DESC' : 'ASC';
                query = `
          DELETE FROM tina_kv
          WHERE (namespace, key) IN (
            SELECT namespace, key FROM tina_kv
            WHERE ${conditions.join(' AND ')}
            ORDER BY key ${orderDirection}
            LIMIT $${paramIndex}
          )
        `;
                params.push(options.limit);
            }

            await this.pool.query(query, params);
            this.nextTick(callback);
        } catch (e) {
            this.nextTick(callback, e as Error);
        }
    }

    _iterator(
        options: IteratorOptions<KDefault>
    ): PostgresIterator<KDefault, VDefault> {
        return new PostgresIterator<KDefault, VDefault>(this, {
            ...options,
            debug: this.debug,
        });
    }
}
