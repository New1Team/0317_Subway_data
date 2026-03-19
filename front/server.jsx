const mariadb = require('mariadb');
const express = require('express');
const app = express();

const pool = mariadb.createPool({
     host: 'localhost', 
     user: 'root', 
     password: '1234',
     database: 'seoul_metro'
});

app.get('/api/points', async (req, res) => {
    let conn;
    try {
        conn = await pool.getConnection();
        // 지도 범위 내 데이터만 가져오거나 전체를 가져옴
        const rows = await conn.query("SELECT id, lat, lng, info FROM your_table_name LIMIT 1000");
        res.json(rows);
    } finally {
        if (conn) conn.end();
    }
});

app.listen(5000, () => console.log('Server running on port 5000'));