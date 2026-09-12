import e from 'express';
import jwt from 'jsonwebtoken';

const JWT_SECRET = 'super_secret_dev_key';

export const authMiddleware = (req,res,next)=>{
    const authHeader = req.headers['authorization'];

    const token = authHeader && authHeader.split(' ')[1];
    
    if(!token){
        return res.status(401).json({error: 'Unauthorized', message: 'No token provided'});
    }

    jwt.verify(token, JWT_SECRET, (err,decoded)=>{
        if(err){
            if(err.name === 'TokenExpiredError'){
                return res.status(401).json({error: 'Unauthorized', message: 'Token expired'});
            } else {
                return res.status(401).json({error: 'Unauthorized', message: 'Invalid token'});
            }

        }
        req.user = decoded;
        next();
    })


}