import { PoolOptions } from 'mysql2';
import mysql from 'mysql2/promise';
import { ExecuterResult, XansqlDialectEngine, XansqlFileConfig } from '@xansql/core';

const MysqlDialect = ({ file, ...config }: PoolOptions & { file?: XansqlFileConfig }) => {
   const pool = mysql.createPool(typeof config === 'string' ? { uri: config } : config);

   const execute = async (sql: string): Promise<ExecuterResult> => {
      const conn = await pool.getConnection();
      try {
         const [rows] = await conn.query(sql);
         const result: any = rows;

         return {
            results: result,
            insertId: result?.insertId ?? 0,
            affectedRows: result?.affectedRows ?? 0
         };
      } finally {
         conn.release();
      }
   };

   const getSchema = async () => {
      const conn = await pool.getConnection();

      try {
         const [tables] = await conn.query<any[]>(
            `SELECT table_name as name
             FROM information_schema.tables 
             WHERE table_schema = DATABASE();`
         );

         const schema: Record<string, any[]> = {};

         for (const t of tables) {
            const table = t.name;
            schema[table] = [];

            // Columns
            const [columns] = await conn.query<any[]>(
               `SELECT 
                  COLUMN_NAME as name,
                  COLUMN_TYPE as type,
                  IS_NULLABLE,
                  COLUMN_DEFAULT,
                  COLUMN_KEY
                FROM information_schema.columns
                WHERE table_schema = DATABASE()
                  AND table_name = ?`,
               [table]
            );

            // Indexes
            const [indexes] = await conn.query<any[]>(
               `SHOW INDEX FROM \`${table}\``
            );

            for (const col of columns) {
               const colName = col.name;

               const isIndexed = indexes.some(i => i.Column_name === colName);
               const isUnique = indexes.some(i => i.Column_name === colName && i.Non_unique === 0);

               schema[table].push({
                  name: colName,
                  type: col.type,
                  notnull: col.IS_NULLABLE === "NO",
                  default_value: col.COLUMN_DEFAULT,
                  pk: col.COLUMN_KEY === "PRI",
                  index: isIndexed,
                  unique: isUnique,
               });
            }
         }

         return schema;
      } finally {
         conn.release();
      }
   };

   return {
      engine: 'mysql' as XansqlDialectEngine,
      execute,
      getSchema,
      file
   };
};

export default MysqlDialect;
