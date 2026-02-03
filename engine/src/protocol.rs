//! Snowflake REST API Protocol Types
//!
//! Snowflake SQL API v2 互換のリクエスト/レスポンス型定義

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// SQL 実行リクエスト
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatementRequest {
    /// 実行する SQL ステートメント
    pub statement: String,

    /// データベース名
    #[serde(default)]
    pub database: Option<String>,

    /// スキーマ名
    #[serde(default)]
    pub schema: Option<String>,

    /// ウェアハウス名（エミュレーターでは無視）
    #[serde(default)]
    pub warehouse: Option<String>,

    /// ロール名（エミュレーターでは無視）
    #[serde(default)]
    pub role: Option<String>,

    /// タイムアウト（秒）
    #[serde(default)]
    pub timeout: Option<u64>,

    /// パラメータバインディング
    #[serde(default)]
    pub bindings: Option<HashMap<String, BindingValue>>,

    /// セッションパラメータ
    #[serde(default)]
    pub parameters: Option<HashMap<String, String>>,

    /// 非同期実行フラグ
    #[serde(default)]
    pub r#async: Option<bool>,
}

/// バインディング値
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BindingValue {
    /// データ型 (FIXED, REAL, TEXT, DATE, TIME, TIMESTAMP, etc.)
    pub r#type: String,

    /// 値（文字列として渡される）
    pub value: String,
}

/// SQL 実行レスポンス
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StatementResponse {
    /// 結果データ（各行は文字列の配列）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Vec<Vec<Option<String>>>>,

    /// 結果セットメタデータ
    pub result_set_meta_data: ResultSetMetaData,

    /// エラーコード（エラー時のみ）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,

    /// エラーメッセージ（エラー時のみ）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// 結果セットメタデータ
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResultSetMetaData {
    /// 行数
    pub num_rows: i64,

    /// 挿入行数
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_rows_inserted: Option<i64>,

    /// 更新行数
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_rows_updated: Option<i64>,

    /// 削除行数
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_rows_deleted: Option<i64>,

    /// ステートメントハンドル
    pub statement_handle: String,

    /// SQL State
    pub sql_state: String,

    /// リクエスト ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,

    /// 列の型情報
    pub row_type: Vec<ColumnMetaData>,

    /// フォーマット
    pub format: String,

    /// パーティション情報
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_info: Option<Vec<PartitionInfo>>,
}

/// 列メタデータ
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnMetaData {
    /// 列名
    pub name: String,

    /// Snowflake データ型
    pub r#type: String,

    /// 精度（数値型の場合）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub precision: Option<i32>,

    /// スケール（数値型の場合）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scale: Option<i32>,

    /// 長さ（文字列型の場合）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub length: Option<i32>,

    /// NULL 許可
    pub nullable: bool,
}

/// パーティション情報
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PartitionInfo {
    /// 行数
    pub row_count: i64,

    /// 非圧縮サイズ
    pub uncompressed_size: i64,
}

/// 非同期実行レスポンス（HTTP 202）
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AsyncResponse {
    /// ステータス確認 URL
    pub statement_status_url: String,

    /// リクエスト ID
    pub request_id: String,

    /// ステートメントハンドル
    pub statement_handle: String,
}

/// エラーレスポンス
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorResponse {
    /// エラーコード
    pub code: String,

    /// エラーメッセージ
    pub message: String,

    /// SQL State
    pub sql_state: String,

    /// ステートメントハンドル（存在する場合）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub statement_handle: Option<String>,
}

impl StatementResponse {
    /// 成功レスポンスを作成
    pub fn success(
        data: Vec<Vec<Option<String>>>,
        row_type: Vec<ColumnMetaData>,
        statement_handle: String,
    ) -> Self {
        let num_rows = data.len() as i64;
        Self {
            data: Some(data),
            result_set_meta_data: ResultSetMetaData {
                num_rows,
                num_rows_inserted: None,
                num_rows_updated: None,
                num_rows_deleted: None,
                statement_handle,
                sql_state: "00000".to_string(),
                request_id: None,
                row_type,
                format: "jsonv2".to_string(),
                partition_info: None,
            },
            code: None,
            message: None,
        }
    }

    /// DML レスポンスを作成
    pub fn dml(affected_rows: i64, statement_handle: String, operation: DmlOperation) -> Self {
        let (inserted, updated, deleted) = match operation {
            DmlOperation::Insert => (Some(affected_rows), None, None),
            DmlOperation::Update => (None, Some(affected_rows), None),
            DmlOperation::Delete => (None, None, Some(affected_rows)),
        };

        Self {
            data: Some(vec![vec![Some(affected_rows.to_string())]]),
            result_set_meta_data: ResultSetMetaData {
                num_rows: 1,
                num_rows_inserted: inserted,
                num_rows_updated: updated,
                num_rows_deleted: deleted,
                statement_handle,
                sql_state: "00000".to_string(),
                request_id: None,
                row_type: vec![ColumnMetaData {
                    name: "number of rows affected".to_string(),
                    r#type: "FIXED".to_string(),
                    precision: Some(38),
                    scale: Some(0),
                    length: None,
                    nullable: false,
                }],
                format: "jsonv2".to_string(),
                partition_info: None,
            },
            code: None,
            message: None,
        }
    }
}

/// DML 操作の種類
pub enum DmlOperation {
    Insert,
    Update,
    Delete,
}
