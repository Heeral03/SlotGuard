// Purpose: if a client's request times out on the network side but actually
// succeeded on the server, a naive retry could re-run business logic a
// second time. This middleware lets a client attach an "Idempotency-Key"
// header (a UUID it generates once per logical operation, and re-sends
// unchanged on any retry). The server remembers what it returned for that
// key, and on a repeat, returns the exact same response instead of
// re-running anything.



export function createIdempotencyMiddleware(redis,ttlSeconds = 86400){
   return async function (req,res,next){

      const key = req.header('Idempotency-Key');

      if(!key){
         return res.status(400).json({error: 'Idempotency-Key header is required'})

      }

      const cacheKey = `idempotency:${req.user.id}:${key}`;
      
      try{
         const cached = await redis.get(cacheKey);
         if(cached){
            const {status,body} = JSON.parse(cached);
            return res.status(status).json(body);
         }
      }
      catch(err){
          console.error('Idempotency check error:', err);
      }


      req.cacheIdempotentResponse = async (status,body)=>{
            try{
               await redis.set(cacheKey, JSON.stringify({status,body}), 'EX',ttlSeconds);

            }
            catch(err){
               console.error('Idempotency cache write error:', err);
            }
      }

      next();


   };

}