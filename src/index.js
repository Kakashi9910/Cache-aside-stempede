// import express from "express";
// import pool from "./db.js";

// const app = express();

// app.get("/test-db", async (req, res) => {
//   try {
//     const result = await pool.query("SELECT NOW()");
    
//     res.json({
//       message: "Connected to Neon PostgreSQL",
//       time: result.rows[0],
//     });
//   } catch (error) {
//     console.error(error);
//     res.status(500).json({ error: "Database connection failed" });
//   }
// });



// app.listen(3000, () => {
//   console.log("Server running on port 3000");
// });


import express from "express";
import dotenv from "dotenv";

import pool from "./db.js";
import redis from "./redis.js";

dotenv.config();

const app = express();

app.use(express.json());

const PORT = 3000;

// Cache TTL
const CACHE_TTL = 10;

// Lock TTL
const LOCK_TTL = 5;


// --------------------------------------------------
// GET USER
// --------------------------------------------------

app.get("/users/:id", async (req, res) => {
  const userId = req.params.id;

  const cacheKey = `user:${userId}`;
  const lockKey = `lock:user:${userId}`;

  try {

    // --------------------------------------------------
    // 1. CHECK REDIS
    // --------------------------------------------------

    const cachedUser = await redis.get(cacheKey);

    if (cachedUser) {

      console.log("CACHE HIT");

      return res.json({
        source: "redis",
        data: JSON.parse(cachedUser),
      });
    }

    console.log("CACHE MISS");


    // --------------------------------------------------
    // 2. TRY TO ACQUIRE LOCK
    // --------------------------------------------------

    const lockAcquired = await redis.set(
      lockKey,
      "locked",
      "NX",
      "EX",
      LOCK_TTL
    );


    // --------------------------------------------------
    // 3. WE GOT THE LOCK
    // --------------------------------------------------

    if (lockAcquired === "OK") {

      console.log("LOCK ACQUIRED");

      try {

        // Double-check cache
        // Another request might have populated it
        // before we acquired the lock.

        const cachedAgain = await redis.get(cacheKey);

        if (cachedAgain) {

          console.log("CACHE FILLED BY ANOTHER REQUEST");

          return res.json({
            source: "redis",
            data: JSON.parse(cachedAgain),
          });
        }


        // --------------------------------------------------
        // 4. QUERY DATABASE
        // --------------------------------------------------

        console.log("QUERYING POSTGRES");

        const result = await pool.query(
          "SELECT * FROM users WHERE id = $1",
          [userId]
        );

        if (result.rows.length === 0) {

          return res.status(404).json({
            message: "User not found",
          });
        }

        const user = result.rows[0];


        // --------------------------------------------------
        // 5. STORE RESULT IN REDIS
        // --------------------------------------------------

        await redis.set(
          cacheKey,
          JSON.stringify(user),
          "EX",
          CACHE_TTL
        );

        console.log("DATA STORED IN CACHE");


        // --------------------------------------------------
        // 6. RETURN RESPONSE
        // --------------------------------------------------

        return res.json({
          source: "postgres",
          data: user,
        });

      } finally {

        // --------------------------------------------------
        // 7. RELEASE LOCK
        // --------------------------------------------------

        await redis.del(lockKey);

        console.log("LOCK RELEASED");
      }
    }


    // --------------------------------------------------
    // 8. SOMEONE ELSE HAS THE LOCK
    // --------------------------------------------------

    console.log("ANOTHER REQUEST IS LOADING DATA");

    // Wait for the other request to populate Redis

    await new Promise((resolve) => {
      setTimeout(resolve, 100);
    });


    // --------------------------------------------------
    // 9. CHECK CACHE AGAIN
    // --------------------------------------------------

    const cachedAfterWait = await redis.get(cacheKey);

    if (cachedAfterWait) {

      console.log("CACHE NOW AVAILABLE");

      return res.json({
        source: "redis-after-wait",
        data: JSON.parse(cachedAfterWait),
      });
    }


    // --------------------------------------------------
    // 10. STILL NOT AVAILABLE
    // --------------------------------------------------

    return res.status(503).json({
      message: "Please try again",
    });

  } catch (error) {

    console.error(error);

    return res.status(500).json({
      message: "Internal server error",
    });
  }
});


app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});