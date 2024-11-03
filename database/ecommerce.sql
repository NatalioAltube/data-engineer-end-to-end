CREATE TABLE transactions (
    InvoiceNo VARCHAR(20),          -- Identificador como string
    StockCode VARCHAR(20),          -- Código de producto como string
    Description TEXT,               -- Descripción del producto como string
    Quantity INT,                   -- Cantidad vendida como entero
    InvoiceDate DATETIME,           -- Fecha de la factura como datetime
    UnitPrice FLOAT,                -- Precio unitario como float
    CustomerID VARCHAR(20),         -- Identificador del cliente como string
    Country VARCHAR(50),            -- País como string
    Revenue FLOAT,                  -- Ingreso total por fila como float
    Month INT                       -- Mes extraído de la fecha como entero
);
