use sf;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
extern "C" {
    pub fn alert(s: &str);
}

#[wasm_bindgen]
pub fn greet(name: &str) {
    alert(&format!("Hello, {}!", name));
}

#[wasm_bindgen]
pub fn exec_query(query: &str) -> String {
    let stmt = sf::parser::query_to_statment(&query).unwrap();
    let plan = sf::plan::statement_to_plan(&stmt).unwrap();
    let rows = plan.execute_plan().unwrap();
    let string_rows = rows
        .rowsets
        .iter()
        .map(|r| r.join("|"))
        .collect::<Vec<String>>()
        .join("|");
    let string_rowtypesname = rows
        .rowtypes
        .iter()
        .map(|r| format!("{}", r.name))
        .collect::<Vec<String>>()
        .join("|");
    let string_rowtypes = rows
        .rowtypes
        .iter()
        .map(|r| format!("{}", r.r#type))
        .collect::<Vec<String>>()
        .join("|");
    format!(
        "rows: {}
        name: {}
        types: {}",
        string_rows, string_rowtypesname, string_rowtypes
    )
}
