// === Constraints (Neo4j 5 syntax) ===
CREATE CONSTRAINT customer_id IF NOT EXISTS
FOR (c:Customer) REQUIRE c.id IS UNIQUE;

CREATE CONSTRAINT product_id IF NOT EXISTS
FOR (p:Product) REQUIRE p.id IS UNIQUE;

CREATE CONSTRAINT category_id IF NOT EXISTS
FOR (g:Category) REQUIRE g.id IS UNIQUE;

CREATE CONSTRAINT order_id IF NOT EXISTS
FOR (o:Order) REQUIRE o.id IS UNIQUE;

// === Helpful property indexes ===
CREATE INDEX product_name_index IF NOT EXISTS
FOR (p:Product) ON (p.name);

CREATE INDEX category_name_index IF NOT EXISTS
FOR (g:Category) ON (g.name);
