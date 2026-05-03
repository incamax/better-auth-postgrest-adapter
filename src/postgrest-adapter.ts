import { createAdapterFactory } from "better-auth/adapters";
import { BetterAuthError } from "@better-auth/core/error";
import type {
  PostgrestClient,
  PostgrestFilterBuilder
} from "@supabase/postgrest-js";
import type {
	AdapterFactoryOptions,
	DBAdapterDebugLogOption,
	JoinConfig,
	Where,
} from "@better-auth/core/db/adapter";

export interface PostgrestFilter {
  col: string;
  val: any;
  op: string;
}

export interface PostgrestRpcParams {
  target_schema: string;
  target_table: string;
  op_type: "select" | "insert" | "update" | "delete";
  filters?: PostgrestFilter[];
  payload?: Record<string, any>;
}

export interface PostgrestAdapterConfig {
  /**
   * Name of PostgreSQL function
   * 
   * @default undefined
   */
  functionName?: string;

  /**
   * Name of PostgreSQL schema in which function was created
   * 
   * @default undefined
   */
  functionSchema?: string;

  /**
   * Whether to execute multiple operations in a transaction.
   * 
   * set this to `false` and operations will be executed sequentially.
   * @default false
   */
  transaction?: boolean | undefined;

  /**
   * Use plural table names
   * 
   * @default false
   */
  usePlural?: boolean;

  /**
   * Enable debug logs for the adapter
   * 
   * @default false
   */
  debugLogs?: DBAdapterDebugLogOption;
   
}

export const postgrestAdapter = (
  postgrest: PostgrestClient,
  config: PostgrestAdapterConfig = {}
) => {
  const adapterConfig: AdapterFactoryOptions['config'] = {
    adapterId: "postgrest-adapter",
    adapterName: "PostgREST Adapter",
    supportsJSON: true,
    supportsDates: true,
    supportsBooleans: true,
    supportsNumericIds: true,
    usePlural: config.usePlural ?? false,
    transaction: false,
    debugLogs: config.debugLogs ?? false,
  };

  return createAdapterFactory({
    config: adapterConfig,
    adapter: (helpers) => {
      const {
        getFieldName,
        getModelName,
        getDefaultModelName,
        transformInput,
        transformOutput,
        schema,
      } = helpers;
      const functionName = config.functionName || "better_auth_postgrest_adapter";
      const functionSchema = config.functionSchema || "public";

      const getJoinKeyName = (baseModel: string, joinedModel: string): string => {
        try {
          const defaultBaseModelName = getDefaultModelName(baseModel);
          const defaultJoinedModelName = getDefaultModelName(joinedModel);
          const key = getModelName(joinedModel).toLowerCase();
          let foreignKeys = Object.entries(
            schema[defaultJoinedModelName]?.fields || {},
          ).filter(
            ([_field, fieldAttributes]: any) =>
              fieldAttributes.references &&
              getDefaultModelName(fieldAttributes.references.model) ===
                defaultBaseModelName,
          );
          if (foreignKeys.length > 0) {
            const [_foreignKey, foreignKeyAttributes] = foreignKeys[0] as any;
            const isUnique = foreignKeyAttributes?.unique === true;
            return isUnique || config.usePlural === true ? key : `${key}s`;
          }
          foreignKeys = Object.entries(
            schema[defaultBaseModelName]?.fields || {},
          ).filter(
            ([_field, fieldAttributes]: any) =>
              fieldAttributes.references &&
              getDefaultModelName(fieldAttributes.references.model) ===
                defaultJoinedModelName,
          );
          if (foreignKeys.length > 0) {
            return key;
          }
        } catch {
          // Fallback
        }
        return `${getModelName(joinedModel).toLowerCase()}s`;
      };

      const convertSelect = (model: string, select?: string[], join?: JoinConfig) => {
        const baseFields = select
          ? select.map((field) => getFieldName({ model, field }))
          : ["*"];
        if (!join) return baseFields.join(",");
        const joinStrings = Object.entries(join).map(([joinModel]) => {
          const key = getJoinKeyName(model, joinModel);
          return `${key}(*)`;
        });
        return [...baseFields, ...joinStrings].join(",");
      };

      const rpc = async (params: PostgrestRpcParams) => {
        const { data, error } = await postgrest.rpc(functionName, params);
        if (error) {
          if (params.op_type === "delete" && (error.code === "P2025" || error.message.includes("not exist"))) return null;
          throw new BetterAuthError(error.message);
        }
        return data;
      };

      const buildFilters = (model: string, where?: Where[]): PostgrestFilter[] => {
        if (!where) return [];
        return where.map(w => ({
          col: getFieldName({ model, field: w.field }),
          val: w.value,
          op: (w.operator as any) || "eq"
        }));
      };

      const applyFilters = (
        query: PostgrestFilterBuilder<any, any, any, any, any, any, any>,
        model: string,
        where?: Where[]
      ): PostgrestFilterBuilder<any, any, any, any, any, any, any> => {
        if (!where) {
          return query;
        }

        let currentQuery = query;

        for (const w of where) {
          const fieldName = getFieldName({ model, field: w.field });
          const operator = w.operator || "eq";
          const value = w.value;

          switch (operator) {
            case "eq":
              currentQuery = currentQuery.eq(fieldName, value);
              break;
            case "ne":
              currentQuery = currentQuery.neq(fieldName, value);
              break;
            case "gt":
              currentQuery = currentQuery.gt(fieldName, value);
              break;
            case "gte":
              currentQuery = currentQuery.gte(fieldName, value);
              break;
            case "lt":
              currentQuery = currentQuery.lt(fieldName, value);
              break;
            case "lte":
              currentQuery = currentQuery.lte(fieldName, value);
              break;
            case "in":
              currentQuery = currentQuery.in(fieldName, Array.isArray(value)
                ? value
                : [value]);
              break;
            case "not_in":
              const values = Array.isArray(value) 
                ? value.join(",") 
                : String(value);
              currentQuery = currentQuery.not(fieldName, "in", `(${values})`);
              break;
            case "contains":
              currentQuery = currentQuery.ilike(fieldName, `%${value}%`);
              break;
            case "starts_with":
              currentQuery = currentQuery.ilike(fieldName, `${value}%`);
              break;
            case "ends_with":
              currentQuery = currentQuery.ilike(fieldName, `%${value}`);
              break;
            default:
              currentQuery = currentQuery.eq(fieldName, value);
          }
        }
        return currentQuery;
      };


      const transactionAdapter = {
        async create({ model, data, select }: any) {
          const result = await rpc({
            target_schema: functionSchema,
            target_table: model,
            op_type: "insert",
            payload: transformInput(data as any, getDefaultModelName(model) as any, "create") as Record<string, any>,
          });
          const record = (Array.isArray(result) ? result[0] : result) as Record<string, any>;
          if (!record) {
            throw new BetterAuthError("Failed to create record");
          }
          return transformOutput(record as any, getDefaultModelName(model) as any, select) as any;
        },
        async findOne({ model, where, select }: any) {
          const results = await rpc({
            target_schema: functionSchema,
            target_table: model,
            op_type: "select",
            filters: buildFilters(model, where),
          });
          const result = Array.isArray(results) ? results[0] : results;
          return (result ? transformOutput(result as any, getDefaultModelName(model) as any, select) : null) as any;
        },
        async findMany({ model, where }: any) {
          const results = await rpc({
            target_schema: functionSchema,
            target_table: model,
            op_type: "select",
            filters: buildFilters(model, where),
          });
          return (Array.isArray(results) ? results : []).map(item => transformOutput(item as any, getDefaultModelName(model) as any)) as any;
        },
        async update({ model, where, update }: any) {
          const results = await rpc({
            target_schema: functionSchema,
            target_table: model,
            op_type: "update",
            filters: buildFilters(model, where),
            payload: transformInput(update as any, getDefaultModelName(model) as any, "update") as Record<string, any>,
          });
          const result = Array.isArray(results) ? results[0] : results;
          if (!result) {
            throw new BetterAuthError("Failed to update record");
          }
          return transformOutput(result as any, getDefaultModelName(model) as any) as any;
        },
        async updateMany({ model, where, update }: any) {
          const results = await rpc({
            target_schema: functionSchema,
            target_table: model,
            op_type: "update",
            filters: buildFilters(model, where),
            payload: update as Record<string, any>,
          });
          return Array.isArray(results) ? results.length : 0;
        },
        async delete({ model, where }: any) {
          const results = await rpc({
            target_schema: functionSchema,
            target_table: model,
            op_type: "delete",
            filters: buildFilters(model, where),
          });
          const result = Array.isArray(results) ? results[0] : results;
          return (result ? transformOutput(result as any, getDefaultModelName(model) as any) : null) as any;
        },
        async deleteMany({ model, where }: any) {
          const results = await rpc({
            target_schema: functionSchema,
            target_table: model,
            op_type: "delete",
            filters: buildFilters(model, where),
          });
          return Array.isArray(results) ? results.length : 0;
        },
        async count({ model, where }: any) {
          const results = await rpc({
            target_schema: functionSchema,
            target_table: model,
            op_type: "select",
            filters: buildFilters(model, where),
          });
          return Array.isArray(results) ? results.length : 0;
        }
      };

      const adapter = {
        async create({ data, model, select }: any) {
          const { data: result, error } = await postgrest
            .from(getModelName(model))
            .insert([transformInput(data as any, getDefaultModelName(model) as any, "create")])
            .select(convertSelect(model, select))
            .single();
          if (error) throw new BetterAuthError(error.message);
          return transformOutput(result as any, getDefaultModelName(model) as any, select) as any;
        },
        async update({ model, where, update }: any) {
          let query = postgrest.from(getModelName(model)).update(transformInput(
            update as Record<string, unknown>,
            getDefaultModelName(model) as any,
            "update"
          ));
          query = applyFilters(query, model, where);
          const { data, error } = await query.select().single();
          if (error) throw new BetterAuthError(error.message);
          return transformOutput(data as any, getDefaultModelName(model) as any) as any;
        },
        async updateMany({ model, where, update }: any) {
          let query = postgrest.from(getModelName(model)).update(update);
          query = applyFilters(query, model, where);
          const { data, error } = await query.select();
          if (error) throw new BetterAuthError(error.message);
          return data?.length || 0;
        },
        async delete({ model, where }: any) {
          let query = postgrest.from(getModelName(model)).delete();
          query = applyFilters(query, model, where);
          const { data, error } = await query.select().maybeSingle();
          if (error) throw new BetterAuthError(error.message);
          return (data ? transformOutput(data as any, getDefaultModelName(model) as any) : null) as any;
        },
        async deleteMany({ model, where }: any) {
          let query = postgrest.from(getModelName(model)).delete();
          query = applyFilters(query, model, where);
          const { data, error } = await query.select();
          if (error) throw new BetterAuthError(error.message);
          return data?.length || 0;
        },
        async findOne({ model, where, select, join }: any) {
          let query = postgrest.from(getModelName(model)).select(convertSelect(model, select, join));
          query = applyFilters(query, model, where);
          const { data, error } = await query.maybeSingle();
          if (error) throw new BetterAuthError(error.message);
          return (data ? transformOutput(data as any, getDefaultModelName(model) as any, select) : null) as any;
        },
        async findMany({ model, where, limit, sortBy, offset, join }: any) {
          let query = postgrest.from(getModelName(model)).select(convertSelect(model, undefined, join));
          query = applyFilters( query, model, where);
          if (limit) query = query.limit(limit);
          if (offset) query = query.range(offset, offset + (limit || 100) - 1);
          if (sortBy && sortBy.length) {
            for (const s of sortBy) {
              query = query.order(getFieldName({ model, field: s.field }), { ascending: s.direction !== "desc" });
            }
          }
          const { data, error } = await query;
          if (error) throw new BetterAuthError(error.message);
          return (data || []).map(item => transformOutput(item as any, getDefaultModelName(model) as any)) as any;
        },
        async count({ model, where }: any) {
          let query = postgrest.from(getModelName(model)).select("*", { count: "exact", head: true });
          query = applyFilters(query, model, where);
          const { count, error } = await query;
          if (error) throw new BetterAuthError(error.message);
          return count || 0;
        },
        transaction: (cb: any) => cb(transactionAdapter)
      };
      return adapter as any;
    },
  });
};
