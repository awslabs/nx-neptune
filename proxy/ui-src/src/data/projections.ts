export interface SavedProjection {
  id: string;
  name: string;
  catalog: string;
  database: string;
  nodeQuery: string;
  edgeQuery: string;
  s3Bucket: string;
  project: string | null;
  addedDate: string;
}

let projections: SavedProjection[] = [
  {
    id: "proj-1",
    name: "Styles Graph",
    catalog: "AwsDataCatalog",
    database: "nx_neptune_styles",
    nodeQuery: 'SELECT id AS "~id", article_type AS "~label", master_category, subcategory, base_color, seasons, product_display_name FROM styles;',
    edgeQuery: 'SELECT purchase_id AS "~id", \'PURCHASED\' AS "~label", customer_id AS "~to", product_id AS "~from", amount, purchase_date FROM purchases;',
    s3Bucket: "s3://my-bucket/",
    project: "MyProject",
    addedDate: "2026-06-22",
  },
  {
    id: "proj-2",
    name: "Social Network",
    catalog: "AwsDataCatalog",
    database: "social_db",
    nodeQuery: 'SELECT user_id AS "~id", \'User\' AS "~label", name, email FROM users;',
    edgeQuery: 'SELECT rel_id AS "~id", \'FOLLOWS\' AS "~label", follower_id AS "~from", followee_id AS "~to" FROM follows;',
    s3Bucket: "s3://social-staging/",
    project: "MyProject",
    addedDate: "2026-06-18",
  },
  {
    id: "proj-3",
    name: "Supply Chain",
    catalog: "AwsDataCatalog",
    database: "supply_chain_db",
    nodeQuery: 'SELECT warehouse_id AS "~id", \'Warehouse\' AS "~label", location, capacity FROM warehouses;',
    edgeQuery: 'SELECT shipment_id AS "~id", \'SHIPS_TO\' AS "~label", origin_id AS "~from", dest_id AS "~to", quantity FROM shipments;',
    s3Bucket: "s3://supply-chain-staging/",
    project: null,
    addedDate: "2026-06-10",
  },
];

let listeners: (() => void)[] = [];

export function getProjections() {
  return [...projections];
}

export function addProjection(p: Omit<SavedProjection, "id" | "addedDate">) {
  projections = [{ ...p, id: `proj-${Date.now()}`, addedDate: new Date().toISOString().slice(0, 10) }, ...projections];
  listeners.forEach(fn => fn());
}

export function deleteProjection(id: string) {
  projections = projections.filter(p => p.id !== id);
  listeners.forEach(fn => fn());
}

export function subscribe(fn: () => void) {
  listeners.push(fn);
  return () => { listeners = listeners.filter(l => l !== fn); };
}
