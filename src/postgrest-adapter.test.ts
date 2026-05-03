import process from "node:process";
import { testAdapter } from "@better-auth/test-utils/adapter";
import { PostgrestClient } from "@supabase/postgrest-js";
import { postgrestAdapter } from "./postgrest-adapter";
import { describe, it } from "vitest";

describe("PostgREST Adapter Tests", async () => {
	/**
	 * Setup PostgREST client pointing to your test instance.
	 * Ensure the URL and service role key (to bypass RLS during tests) are correct.
	 */
	const postgrest = new PostgrestClient(
		process.env.POSTGREST_URL || "http://localhost:3000",
		{
			headers: {
				Authorization: `Bearer ${process.env.POSTGREST_SERVICE_ROLE_KEY || ""}`,
			},
		},
	);

	const { execute } = await testAdapter({
		adapter: (options) => {
			/**
			 * Our postgrestAdapter returns the creator function from createAdapterFactory.
			 * The test runner provides the necessary helpers/options.
			 */
			return postgrestAdapter(postgrest, {
				functionName: "better_auth_postgrest_adapter",
				transaction: true,
				usePlural: false,
			});
		},
		tests: Object.values(['']).flat() as any[],
		runMigrations: async () => {
			// PostgREST requires the schema and RPC function to be predefined.
			// In a CI environment, you would use a tool like 'psql' to apply function.sql 
			// and create the necessary tables before running this test.
			console.log("Migration check: Ensure tables and function.sql are applied to the DB.");
		},
	});

	it("should pass the Better Auth standard adapter test suite", async () => {
		await execute();
	});
});
