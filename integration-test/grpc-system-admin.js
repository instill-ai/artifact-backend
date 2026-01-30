/**
 * System Admin API Integration Test
 *
 * PURPOSE:
 * Validates all System Management Admin APIs for managing system configurations
 * that define how Knowledge Bases are created (AI model family, embedding
 * dimensionality, RAG settings).
 *
 * TEST FLOW:
 * Phase 1: ListSystemsAdmin - Verify default systems exist
 * Phase 2: GetSystemAdmin - Retrieve specific system configs
 * Phase 3: GetDefaultSystemAdmin - Verify default system
 * Phase 4: CreateSystemAdmin - Create custom system
 * Phase 5: UpdateSystemAdmin - Update with field mask
 * Phase 6: RenameSystemAdmin - Change system ID
 * Phase 7: SetDefaultSystemAdmin - Change default system
 * Phase 8: DeleteSystemAdmin - Remove custom system
 * Phase 9: Verify protected systems cannot be deleted/renamed
 *
 * KEY VALIDATIONS:
 * - All CRUD operations work correctly
 * - Field mask properly filters updates
 * - Rename operation changes system ID
 * - Default system management works
 * - Protected "openai" system cannot be deleted/renamed
 * - System config follows RFC-1034 for IDs
 * - System resource name format: systems/{system_id}
 */

import grpc from "k6/net/grpc";
import { check, group } from "k6";

import * as constant from "./const.js";
import * as helper from "./helper.js";
import { grpcInvokeWithRetry } from "./helper.js";

// Use httpRetry for automatic retry on transient errors (429, 5xx)
const http = helper.httpRetry;

const client = new grpc.Client();
client.load(
    ["proto"],
    "artifact/v1alpha/artifact_private_service.proto"
);

export let options = {
    setupTimeout: '300s',
    teardownTimeout: '60s',
    insecureSkipTLSVerify: true,
    thresholds: {
        checks: ["rate == 1.0"],
    },
};

export function setup() {
    check(true, { [constant.banner('System Admin API Test: Setup')]: () => true });

    // Generate unique test prefix
    const dbIDPrefix = constant.generateDBIDPrefix();
    console.log(`grpc-system-admin.js: Using unique test prefix: ${dbIDPrefix}`);

    // Authenticate with retry to handle transient failures
    const authHeader = helper.getBasicAuthHeader(constant.defaultUsername, constant.defaultPassword);
    const grpcMetadata = {
        "metadata": {
            "Authorization": authHeader,
        },
        "timeout": "300s",
    };

    return {
        metadata: grpcMetadata,
        dbIDPrefix: dbIDPrefix,
    };
}

export function teardown(data) {
    // Cleanup any test systems that might remain
    group("System Admin Test: Teardown", () => {
        check(true, { [constant.banner('Teardown')]: () => true });

        // Clean up systems created during the test
        const testSystemIds = [];
        if (data.customSystemId) testSystemIds.push(data.customSystemId);
        if (data.renamedSystemId) testSystemIds.push(data.renamedSystemId);

        for (const systemId of testSystemIds) {
            try {
                grpcInvokeWithRetry(client,
                    "artifact.v1alpha.ArtifactPrivateService/DeleteSystemAdmin",
                    { system_id: systemId },
                    data.metadata
                );
                console.log(`Teardown: Deleted system ${systemId}`);
            } catch (e) {
                // Ignore errors during cleanup (system might already be deleted)
            }
        }

        client.close();
    });
}

export default function (data) {
    // Connect gRPC client to private service
    client.connect(constant.artifactGRPCPrivateHost, {
        plaintext: true,
    });

    // Store system IDs discovered from API (IDs are now sys-{hash} format, not plain strings)
    let openaiSystemId = null;
    let geminiSystemId = null;

    group("System Admin API: Complete Test Suite", () => {

        // ====================================================================
        // PHASE 1: List Systems
        // ====================================================================
        group("Phase 1: List all system configurations", () => {
            console.log("\n=== Phase 1: Listing all systems ===");

            const listRes = grpcInvokeWithRetry(client,
                "artifact.v1alpha.ArtifactPrivateService/ListSystemsAdmin",
                {},
                data.metadata
            );

            check(listRes, {
                "Phase 1: List systems successful": (r) => r && r.status === grpc.StatusOK,
                "Phase 1: Has systems array": (r) => r.message && Array.isArray(r.message.systems),
                "Phase 1: Has at least 2 systems": (r) => r.message?.systems?.length >= 2,
            });

            if (listRes.status === grpc.StatusOK) {
                console.log(`Phase 1: Found ${listRes.message.systems.length} systems`);
                listRes.message.systems.forEach((system, idx) => {
                    console.log(`  ${idx + 1}. System ID: ${system.id}, Slug: ${system.slug || 'N/A'}, DisplayName: ${system.displayName || 'N/A'}, Default: ${system.isDefault || false}`);
                    // Capture system IDs by matching display_name or slug
                    // NOTE: IDs are now sys-{hash} format, slugs/display_names are "openai"/"gemini"
                    const slug = (system.slug || '').toLowerCase();
                    const displayName = (system.displayName || '').toLowerCase();
                    if (slug === 'openai' || displayName === 'openai' || displayName.includes('openai')) {
                        openaiSystemId = system.id;
                        console.log(`Phase 1: Captured OpenAI system ID: ${openaiSystemId}`);
                    }
                    if (slug === 'gemini' || displayName === 'gemini' || displayName.includes('gemini')) {
                        geminiSystemId = system.id;
                        console.log(`Phase 1: Captured Gemini system ID: ${geminiSystemId}`);
                    }
                });
            }
        });

        // ====================================================================
        // PHASE 2: Get Specific Systems
        // ====================================================================
        group("Phase 2: Get specific system configurations", () => {
            console.log("\n=== Phase 2: Getting specific systems ===");

            if (!openaiSystemId) {
                console.log("Phase 2: OpenAI system ID not found in Phase 1, skipping OpenAI tests");
            } else {
                // Get OpenAI system using its canonical ID (sys-{hash} format)
                const openaiRes = grpcInvokeWithRetry(client,
                    "artifact.v1alpha.ArtifactPrivateService/GetSystemAdmin",
                    { system_id: openaiSystemId },
                    data.metadata
                );

                check(openaiRes, {
                    "Phase 2: Get OpenAI system successful": (r) => r && r.status === grpc.StatusOK,
                    "Phase 2: OpenAI system has sys- prefixed ID": (r) => r.message?.system?.id && r.message.system.id.startsWith("sys-"),
                    "Phase 2: OpenAI system has valid resource name": (r) => r.message?.system?.name && r.message.system.name.startsWith("systems/sys-"),
                    "Phase 2: OpenAI system has config": (r) => r.message?.system?.config !== null && r.message?.system?.config !== undefined,
                    "Phase 2: OpenAI config has model_family": (r) =>
                        r.message?.system?.config?.rag?.embedding?.model_family === "openai",
                    "Phase 2: OpenAI config has dimensionality": (r) =>
                        r.message?.system?.config?.rag?.embedding?.dimensionality === 1536,
                });
            }

            if (!geminiSystemId) {
                console.log("Phase 2: Gemini system ID not found in Phase 1, skipping Gemini tests");
            } else {
                // Get Gemini system using its canonical ID
                const geminiRes = grpcInvokeWithRetry(client,
                    "artifact.v1alpha.ArtifactPrivateService/GetSystemAdmin",
                    { system_id: geminiSystemId },
                    data.metadata
                );

                check(geminiRes, {
                    "Phase 2: Get Gemini system successful": (r) => r && r.status === grpc.StatusOK,
                    "Phase 2: Gemini system has sys- prefixed ID": (r) => r.message?.system?.id && r.message.system.id.startsWith("sys-"),
                    "Phase 2: Gemini system has valid resource name": (r) => r.message?.system?.name && r.message.system.name.startsWith("systems/sys-"),
                    "Phase 2: Gemini config has model_family": (r) =>
                        r.message?.system?.config?.rag?.embedding?.model_family === "gemini",
                    "Phase 2: Gemini config has dimensionality": (r) =>
                        r.message?.system?.config?.rag?.embedding?.dimensionality === 3072,
                });
            }
        });

        // ====================================================================
        // PHASE 3: Get Default System
        // ====================================================================
        group("Phase 3: Get default system configuration", () => {
            console.log("\n=== Phase 3: Getting default system ===");

            const defaultRes = grpcInvokeWithRetry(client,
                "artifact.v1alpha.ArtifactPrivateService/GetDefaultSystemAdmin",
                {},
                data.metadata
            );

            check(defaultRes, {
                "Phase 3: Get default system successful": (r) => r && r.status === grpc.StatusOK,
                "Phase 3: Default system has ID": (r) => r.message?.system?.id && r.message.system.id.length > 0,
                "Phase 3: Default system is_default is true": (r) => r.message?.system?.isDefault === true,
            });

            if (defaultRes.status === grpc.StatusOK) {
                console.log(`Phase 3: Default system is: ${defaultRes.message.system.id}`);
            }
        });

        // ====================================================================
        // PHASE 4: Create Custom System
        // ====================================================================
        let customSystemId = null;
        group("Phase 4: Create custom system configuration", () => {
            console.log("\n=== Phase 4: Creating custom system ===");

            // Use unique display_name to avoid conflicts with previous test runs
            // API CHANGE: ID is now auto-generated as "sys-{hash}", use display_name instead
            const customDisplayName = `${data.dbIDPrefix}sysadmin-custom`;

            const createRes = grpcInvokeWithRetry(client,
                "artifact.v1alpha.ArtifactPrivateService/CreateSystemAdmin",
                {
                    system: {
                        // API CHANGE: ID is auto-generated, display_name is required
                        displayName: customDisplayName,
                        config: {
                            rag: {
                                embedding: {
                                    model_family: "openai",
                                    dimensionality: 1536
                                }
                            }
                        },
                        description: "Test custom system for integration testing"
                    }
                },
                data.metadata
            );

            check(createRes, {
                "Phase 4: Create system successful": (r) => r && r.status === grpc.StatusOK,
                // API CHANGE: ID is now auto-generated with sys- prefix
                "Phase 4: Created system has sys- prefixed ID": (r) => r && r.message && r.message.system && r.message.system.id.startsWith("sys-"),
                "Phase 4: Created system has display_name": (r) => r && r.message && r.message.system && r.message.system.displayName === customDisplayName,
                "Phase 4: Created system has resource name": (r) => r && r.message && r.message.system && r.message.system.name.startsWith("systems/sys-"),
                "Phase 4: Created system is not default": (r) => r && r.message && r.message.system && r.message.system.isDefault === false,
                "Phase 4: Created system has timestamps": (r) => r && r.message && r.message.system && r.message.system.createTime && r.message.system.updateTime,
            });

            if (createRes.status === grpc.StatusOK) {
                customSystemId = createRes.message.system.id; // Get auto-generated ID from response
                data.customSystemId = customSystemId; // Store for teardown
                console.log(`Phase 4: Created system with ID: ${customSystemId}, Display Name: ${customDisplayName}`);
            } else {
                console.error(`Phase 4: Create system FAILED - Status: ${createRes.status}, Error: ${createRes.error ? createRes.error.message : 'unknown'}`);
            }
        });

        // ====================================================================
        // PHASE 5: Update System with Field Mask
        // ====================================================================
        group("Phase 5: Update system with field mask", () => {
            console.log("\n=== Phase 5: Updating system description only (field mask) ===");

            if (!customSystemId) {
                console.error("Phase 5: Skipping - custom system ID not available from Phase 4");
                return;
            }

            console.log(`Phase 5: Using customSystemId: ${customSystemId} (type: ${typeof customSystemId})`);

            const updateRequest = {
                system: {
                    id: customSystemId,
                    description: "Updated description via field mask"
                },
                update_mask: "description"  // k6 gRPC uses snake_case string for field mask
            };
            console.log(`Phase 5: Request object: ${JSON.stringify(updateRequest)}`);

            const updateRes = grpcInvokeWithRetry(client,
                "artifact.v1alpha.ArtifactPrivateService/UpdateSystemAdmin",
                updateRequest,
                data.metadata
            );

            if (updateRes.status !== grpc.StatusOK) {
                console.error(`Phase 5: Update FAILED - Status: ${updateRes.status}, Error: ${updateRes.error ? updateRes.error.message : 'unknown'}`);
            }

            check(updateRes, {
                "Phase 5: Update system successful": (r) => r && r.status === grpc.StatusOK,
                "Phase 5: Description was updated": (r) =>
                    r && r.message && r.message.system && r.message.system.description === "Updated description via field mask",
                "Phase 5: Config remains unchanged": (r) =>
                    r && r.message && r.message.system && r.message.system.config &&
                    r.message.system.config.rag.embedding.model_family === "openai" &&
                    r.message.system.config.rag.embedding.dimensionality === 1536,
            });

            // Update both config and description
            console.log("\n=== Phase 5: Updating config with field mask ===");

            const updateConfigRes = grpcInvokeWithRetry(client,
                "artifact.v1alpha.ArtifactPrivateService/UpdateSystemAdmin",
                {
                    system: {
                        id: customSystemId,
                        config: {
                            rag: {
                                embedding: {
                                    model_family: "gemini",
                                    dimensionality: 3072
                                }
                            }
                        },
                        description: "Updated config to Gemini"
                    },
                    update_mask: "config,description"  // Comma-separated string for multiple fields
                },
                data.metadata
            );

            if (updateConfigRes.status !== grpc.StatusOK) {
                console.error(`Phase 5: Config update FAILED - Status: ${updateConfigRes.status}, Error: ${updateConfigRes.error ? updateConfigRes.error.message : 'unknown'}`);
            }

            check(updateConfigRes, {
                "Phase 5: Update config successful": (r) => r && r.status === grpc.StatusOK,
                "Phase 5: Config was updated to Gemini": (r) =>
                    r && r.message && r.message.system && r.message.system.config &&
                    r.message.system.config.rag.embedding.model_family === "gemini",
                "Phase 5: Dimensionality was updated": (r) =>
                    r && r.message && r.message.system && r.message.system.config &&
                    r.message.system.config.rag.embedding.dimensionality === 3072,
                "Phase 5: Description was also updated": (r) =>
                    r && r.message && r.message.system && r.message.system.description === "Updated config to Gemini",
            });
        });

        // ====================================================================
        // PHASE 6: Rename System (Change display_name, NOT the ID)
        // ====================================================================
        group("Phase 6: Rename system configuration (update display_name)", () => {
            console.log("\n=== Phase 6: Updating system display_name (ID is immutable) ===");

            if (!customSystemId) {
                console.error("Phase 6: Skipping - custom system ID not available from Phase 4");
                return;
            }

            // API CHANGE: IDs are immutable, we can only change display_name (and slug)
            const newDisplayName = `${data.dbIDPrefix}sysadmin-renamed`;

            const renameRes = grpcInvokeWithRetry(client,
                "artifact.v1alpha.ArtifactPrivateService/RenameSystemAdmin",
                {
                    system_id: customSystemId,
                    new_display_name: newDisplayName
                },
                data.metadata
            );

            check(renameRes, {
                "Phase 6: Rename system successful": (r) => r && r.status === grpc.StatusOK,
                // API CHANGE: ID is immutable - it should NOT change
                "Phase 6: System ID unchanged (immutable)": (r) => r.message?.system?.id === customSystemId,
                "Phase 6: Resource name unchanged": (r) => r.message?.system?.name === `systems/${customSystemId}`,
                "Phase 6: display_name was updated": (r) => r.message?.system?.displayName === newDisplayName,
                "Phase 6: Config preserved": (r) =>
                    r.message?.system?.config?.rag?.embedding?.model_family === "gemini",
            });

            // Note: renamedSystemId is no longer different from customSystemId since ID is immutable
            // We store for teardown but the value is the same
            if (renameRes.status === grpc.StatusOK) {
                data.renamedSystemId = customSystemId; // ID doesn't change
            }

            // Verify system is still accessible by the same ID (ID is immutable)
            const getRes = grpcInvokeWithRetry(client,
                "artifact.v1alpha.ArtifactPrivateService/GetSystemAdmin",
                { system_id: customSystemId },
                data.metadata
            );

            check(getRes, {
                "Phase 6: System still accessible by same ID": (r) => r && r.status === grpc.StatusOK,
                "Phase 6: Retrieved system has updated display_name": (r) => r.message?.system?.displayName === newDisplayName,
            });
        });

        // ====================================================================
        // PHASE 7: Set Default System
        // ====================================================================
        group("Phase 7: Change default system", () => {
            console.log("\n=== Phase 7: Setting custom system as default ===");

            const renamedSystemId = data.renamedSystemId;
            if (!renamedSystemId) {
                console.error("Phase 7: Skipping - renamed system ID not available from Phase 6");
                return;
            }

            // First, get current default
            const getCurrentDefaultRes = grpcInvokeWithRetry(client,
                "artifact.v1alpha.ArtifactPrivateService/GetDefaultSystemAdmin",
                {},
                data.metadata
            );

            let previousDefaultId = null;
            if (getCurrentDefaultRes.status === grpc.StatusOK) {
                previousDefaultId = getCurrentDefaultRes.message.system.id;
                console.log(`Phase 7: Current default is: ${previousDefaultId}`);
            }

            // Set custom system as default
            const setDefaultRes = grpcInvokeWithRetry(client,
                "artifact.v1alpha.ArtifactPrivateService/SetDefaultSystemAdmin",
                { system_id: renamedSystemId },
                data.metadata
            );

            check(setDefaultRes, {
                "Phase 7: Set default successful": (r) => r && r.status === grpc.StatusOK,
                "Phase 7: System is now default": (r) => r.message?.system?.isDefault === true,
                "Phase 7: Correct system was set": (r) => r.message?.system?.id === renamedSystemId,
            });

            // Verify via GetDefaultSystemAdmin
            const verifyDefaultRes = grpcInvokeWithRetry(client,
                "artifact.v1alpha.ArtifactPrivateService/GetDefaultSystemAdmin",
                {},
                data.metadata
            );

            check(verifyDefaultRes, {
                "Phase 7: GetDefault returns new default": (r) =>
                    r && r.status === grpc.StatusOK &&
                    r.message?.system?.id === renamedSystemId,
            });

            // Restore original default
            if (previousDefaultId && previousDefaultId !== renamedSystemId) {
                console.log(`Phase 7: Restoring original default: ${previousDefaultId}`);
                const restoreRes = grpcInvokeWithRetry(client,
                    "artifact.v1alpha.ArtifactPrivateService/SetDefaultSystemAdmin",
                    { system_id: previousDefaultId },
                    data.metadata
                );

                check(restoreRes, {
                    "Phase 7: Restore original default successful": (r) => r && r.status === grpc.StatusOK,
                });
            }
        });

        // ====================================================================
        // PHASE 8: Delete Custom System
        // ====================================================================
        group("Phase 8: Delete custom system", () => {
            console.log("\n=== Phase 8: Deleting custom system ===");

            const renamedSystemId = data.renamedSystemId;
            if (!renamedSystemId) {
                console.error("Phase 8: Skipping - renamed system ID not available");
                return;
            }

            const deleteRes = grpcInvokeWithRetry(client,
                "artifact.v1alpha.ArtifactPrivateService/DeleteSystemAdmin",
                { system_id: renamedSystemId },
                data.metadata
            );

            check(deleteRes, {
                "Phase 8: Delete system successful": (r) => r && r.status === grpc.StatusOK,
                "Phase 8: Delete returned success": (r) => r.message?.success === true,
            });

            // Verify system is deleted
            const getDeletedRes = grpcInvokeWithRetry(client,
                "artifact.v1alpha.ArtifactPrivateService/GetSystemAdmin",
                { system_id: renamedSystemId },
                data.metadata
            );

            check(getDeletedRes, {
                "Phase 8: Deleted system not accessible": (r) => r && r.status !== grpc.StatusOK,
            });

            // Mark as deleted for teardown
            if (deleteRes.status === grpc.StatusOK) {
                data.renamedSystemId = null;
            }
        });

        // ====================================================================
        // PHASE 9: Verify Protected Systems
        // ====================================================================
        group("Phase 9: Verify protected system cannot be deleted or renamed", () => {
            console.log("\n=== Phase 9: Testing protected 'openai' system ===");

            if (!openaiSystemId) {
                console.error("Phase 9: Skipping - OpenAI system ID not available from Phase 1");
                return;
            }

            console.log(`Phase 9: Using OpenAI system ID: ${openaiSystemId}`);

            // Try to delete openai system (should fail - protected)
            const deleteOpenaiRes = grpcInvokeWithRetry(client,
                "artifact.v1alpha.ArtifactPrivateService/DeleteSystemAdmin",
                { system_id: openaiSystemId },
                data.metadata
            );

            check(deleteOpenaiRes, {
                "Phase 9: Cannot delete openai system (protected)": (r) => r && r.status !== grpc.StatusOK,
            });

            // Try to rename openai system (should fail - protected)
            const renameOpenaiRes = grpcInvokeWithRetry(client,
                "artifact.v1alpha.ArtifactPrivateService/RenameSystemAdmin",
                {
                    system_id: openaiSystemId,
                    new_display_name: "openai-renamed"
                },
                data.metadata
            );

            check(renameOpenaiRes, {
                "Phase 9: Cannot rename openai system (protected)": (r) => r && r.status !== grpc.StatusOK,
            });

            // Verify openai system still exists with same ID
            const getOpenaiRes = grpcInvokeWithRetry(client,
                "artifact.v1alpha.ArtifactPrivateService/GetSystemAdmin",
                { system_id: openaiSystemId },
                data.metadata
            );

            check(getOpenaiRes, {
                "Phase 9: OpenAI system still accessible": (r) => r && r.status === grpc.StatusOK,
                "Phase 9: OpenAI system ID unchanged": (r) => r.message?.system?.id === openaiSystemId,
            });

            console.log("Phase 9: Protected system verification complete");
        });

        console.log("\n=== All System Admin API tests completed successfully ===");
    });
}
