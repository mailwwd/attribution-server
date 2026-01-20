const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
require('dotenv').config();

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3000;

// Database connection
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: {
        rejectUnauthorized: false
    }
});

// Test database connection
pool.connect((err, client, release) => {
    if (err) {
        console.error('Error connecting to database:', err);
    } else {
        console.log('✅ Connected to database!');
        release();
    }
});

// Health check endpoint
app.get('/', (req, res) => {
    res.json({ 
        status: 'running',
        message: 'Attribution Tracking Server',
        timestamp: new Date()
    });
});

// Track conversion endpoint
app.post('/api/track-conversion', async (req, res) => {
    try {
        const data = req.body;
        console.log('Received conversion:', data.orderId);

        // Insert conversion
        const conversionResult = await pool.query(`
            INSERT INTO conversions (
                order_id, order_number, value, subtotal, tax, currency,
                gclid, first_gclid,
                utm_source, utm_medium, utm_campaign, utm_term, utm_content,
                first_utm_source, first_utm_medium, first_utm_campaign,
                attr_source, attr_campaign, attr_adgroup, attr_ad,
                first_attr_source, first_attr_campaign,
                market, domain, shipping_country, billing_country,
                customer_email,
                journey_length, time_to_conversion,
                first_click_at, last_click_at, converted_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, $8,
                $9, $10, $11, $12, $13,
                $14, $15, $16,
                $17, $18, $19, $20,
                $21, $22,
                $23, $24, $25, $26,
                $27,
                $28, $29,
                $30, $31, $32
            ) RETURNING id
        `, [
            data.orderId, data.orderNumber, data.value, data.subtotal, data.tax, data.currency,
            data.gclid, data.firstClickGclid,
            data.utmSource, data.utmMedium, data.utmCampaign, data.utmTerm, data.utmContent,
            data.firstUtmSource, data.firstUtmMedium, data.firstUtmCampaign,
            data.attrSource, data.attrCampaign, data.attrAdgroup, data.attrAd,
            data.firstAttrSource, data.firstAttrCampaign,
            data.market, data.domain, data.shippingCountry, data.billingCountry,
            data.email,
            data.journeyLength, data.timeToConversion,
            data.firstClickTimestamp ? new Date(data.firstClickTimestamp) : null,
            data.lastClickTimestamp ? new Date(data.lastClickTimestamp) : null,
            new Date()
        ]);

        const conversionId = conversionResult.rows[0].id;

        // Insert products
        if (data.products && data.products.length > 0) {
            for (const product of data.products) {
                await pool.query(`
                    INSERT INTO conversion_products (
                        conversion_id, product_name, product_sku, variant_title, quantity, price
                    ) VALUES ($1, $2, $3, $4, $5, $6)
                `, [
                    conversionId,
                    product.name,
                    product.sku,
                    product.variant,
                    product.quantity,
                    product.price
                ]);
            }
        }

        // Insert journey
        if (data.journey && data.journey.length > 0) {
            for (let i = 0; i < data.journey.length; i++) {
                const step = data.journey[i];
                await pool.query(`
                    INSERT INTO customer_journey (
                        conversion_id, url, path, title, page_type, referrer, 
                        visited_at, sequence_number
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                `, [
                    conversionId,
                    step.url,
                    step.path,
                    step.title,
                    step.pageType,
                    step.referrer,
                    new Date(step.timestamp),
                    i + 1
                ]);
            }
        }

        console.log('✅ Conversion saved:', data.orderId);

        res.json({
            success: true,
            conversionId: conversionId,
            orderId: data.orderId
        });

    } catch (error) {
        console.error('❌ Error saving conversion:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Get conversions by campaign
app.get('/api/conversions/by-campaign', async (req, res) => {
    try {
        const { market, startDate, endDate } = req.query;

        let query = `
            SELECT 
                utm_campaign,
                attr_campaign,
                market,
                COUNT(*) as conversions,
                SUM(value) as revenue,
                AVG(journey_length) as avg_journey_length,
                AVG(time_to_conversion) as avg_time_to_conversion
            FROM conversions
            WHERE converted_at >= $1 AND converted_at <= $2
        `;

        const params = [
            startDate || '2024-01-01',
            endDate || new Date().toISOString()
        ];

        if (market && market !== 'all') {
            query += ` AND market = $3`;
            params.push(market);
        }

        query += ` GROUP BY utm_campaign, attr_campaign, market ORDER BY revenue DESC`;

        const result = await pool.query(query, params);

        res.json({
            success: true,
            data: result.rows
        });

    } catch (error) {
        console.error('Error fetching conversions:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Get order details
app.get('/api/order/:orderId', async (req, res) => {
    try {
        const { orderId } = req.params;

        const conversionResult = await pool.query(`
            SELECT * FROM conversions WHERE order_id = $1
        `, [orderId]);

        if (conversionResult.rows.length === 0) {
            return res.status(404).json({
                success: false,
                error: 'Order not found'
            });
        }

        const conversion = conversionResult.rows[0];

        const productsResult = await pool.query(`
            SELECT * FROM conversion_products WHERE conversion_id = $1
        `, [conversion.id]);

        const journeyResult = await pool.query(`
            SELECT * FROM customer_journey 
            WHERE conversion_id = $1 
            ORDER BY sequence_number
        `, [conversion.id]);

        res.json({
            success: true,
            order: {
                ...conversion,
                products: productsResult.rows,
                journey: journeyResult.rows
            }
        });

    } catch (error) {
        console.error('Error fetching order:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Start server
app.listen(PORT, () => {
    console.log(`🚀 Server running on port ${PORT}`);
    console.log(`📊 API endpoints:`);
    console.log(`   POST /api/track-conversion`);
    console.log(`   GET  /api/conversions/by-campaign`);
    console.log(`   GET  /api/order/:orderId`);
});
