CREATE TABLE Transactions (
    transactionId VARCHAR(36) PRIMARY KEY,      
    userId VARCHAR(50),                         
    merchantId VARCHAR(50),                     
    amount DOUBLE PRECISION,                    
    transactionTime BIGINT,                   
    transactionType VARCHAR(50),                
    location VARCHAR(50),                       
    paymentMethod VARCHAR(50),                  
    isInternational VARCHAR(25),                
    currency VARCHAR(50)                        
);



drop table Transactions


SELECT * FROM Transactions
SELECT COUNT(*) from Transactions
