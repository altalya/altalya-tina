/**
 * Quick script to test PostgreSQL connection
 * Run with: node --experimental-specifier-resolution=node test-postgres.js
 */
require('dotenv').config();
const { Pool } = require('pg');

const connectionString = process.env.POSTGRES_URL;

if (!connectionString) {
    console.error('❌ POSTGRES_URL environment variable is not set!');
    process.exit(1);
}

console.log('🔍 Testing PostgreSQL connection...');
console.log(`   URL: ${connectionString.replace(/:[^:@]*@/, ':****@')}`);

const pool = new Pool({ connectionString });

async function testConnection() {
    try {
        // Test basic connection
        const client = await pool.connect();
        console.log('✅ Connected to PostgreSQL successfully!');

        // Check PostgreSQL version
        const versionResult = await client.query('SELECT version()');
        console.log(`   PostgreSQL version: ${versionResult.rows[0].version.split(',')[0]}`);

        // Check if tina_kv table exists
        const tableCheck = await client.query(`
      SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_name = 'tina_kv'
      );
    `);

        if (tableCheck.rows[0].exists) {
            console.log('✅ tina_kv table exists');

            // Count rows
            const countResult = await client.query('SELECT COUNT(*) FROM tina_kv');
            console.log(`   Rows in tina_kv: ${countResult.rows[0].count}`);
        } else {
            console.log('ℹ️  tina_kv table does not exist yet (will be created on first TinaCMS connection)');
        }

        client.release();
        await pool.end();

        console.log('\n🎉 PostgreSQL connection test passed!');
    } catch (error) {
        console.error('❌ Connection failed:', error.message);
        if (error.code === 'ECONNREFUSED') {
            console.error('   → Make sure PostgreSQL is running and accessible');
        } else if (error.code === '28P01') {
            console.error('   → Invalid username or password');
        } else if (error.code === '3D000') {
            console.error('   → Database does not exist');
        }
        process.exit(1);
    }
}

testConnection();
