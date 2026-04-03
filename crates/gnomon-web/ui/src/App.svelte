<script>
  import { onMount } from "svelte";
  import SunburstChart from "./components/SunburstChart.svelte";

  const ROOT_OPTIONS = ["project", "category"];
  const LENS_OPTIONS = ["uncached_input", "gross_input", "output", "total"];
  const ROOT_LABELS = {
    project: "Project hierarchy",
    category: "Category hierarchy",
  };
  const LENS_LABELS = {
    uncached_input: "Uncached input",
    gross_input: "Gross input",
    output: "Output",
    total: "Total",
  };
  const TIME_WINDOW_OPTIONS = [
    { key: "all", label: "All time" },
    { key: "day", label: "Last 24h" },
    { key: "week", label: "Last 7d" },
  ];

  function createRootContext(root = "project", lens = "uncached_input") {
    return {
      root,
      lens,
      path: "root",
      project_id: null,
      category: null,
      classification_state: null,
      normalized_action: null,
      command_family: null,
      base_command: null,
      parent_path: null,
      model: null,
      filter_project_id: null,
      filter_category: null,
      time_window: "all",
    };
  }

  let status = null;
  let browseReport = null;
  let selectedRow = null;
  let detail = null;
  let loading = true;
  let loadingDetail = false;
  let refreshing = false;
  let error = "";
  let detailError = "";
  let shortcutMessage = "";
  let liveMessage = "";
  let filters = null;
  let currentContext = createRootContext();
  let contextHistory = [];
  let focusedPane = "browse";
  let detailVisible = true;
  let showBreadcrumbs = true;
  let rowFilter = "";
  let jumpQuery = "";
  let projectFilterValue = "";
  let categoryFilterValue = "";
  let rowFilterInput;
  let jumpInput;
  let browsePanel;
  let detailPanel;
  let opportunityOnly = false;
  let compactRows = false;
  let childRowsByParent = {};
  let sunburstLoading = false;
  let prefetchEpoch = 0;

  $: projectFilterValue = currentContext.filter_project_id == null ? "" : String(currentContext.filter_project_id);
  $: categoryFilterValue = currentContext.filter_category ?? "";

  function copyContext(context) {
    return structuredClone(context);
  }

  function announce(message) {
    liveMessage = message;
  }

  function focusBrowsePanel() {
    browsePanel?.focus();
  }

  function focusDetailPanel() {
    detailPanel?.focus();
  }

  function timeWindowBounds(key) {
    if (key === "all") {
      return { start_at_utc: null, end_at_utc: null };
    }

    const end = new Date();
    const start = new Date(end.getTime() - (key === "day" ? 24 : 24 * 7) * 60 * 60 * 1000);
    return {
      start_at_utc: start.toISOString(),
      end_at_utc: end.toISOString(),
    };
  }

  function buildBrowseParams(context) {
    const params = new URLSearchParams();
    params.set("root", context.root);
    params.set("lens", context.lens);
    params.set("path", context.path);

    if (context.project_id != null) {
      params.set("project_id", String(context.project_id));
    } else if (context.filter_project_id != null) {
      params.set("project_id", String(context.filter_project_id));
    }

    for (const key of [
      "category",
      "classification_state",
      "normalized_action",
      "command_family",
      "base_command",
      "parent_path",
      "model",
    ]) {
      const value = context[key];
      if (value !== null && value !== undefined && value !== "") {
        params.set(key, String(value));
      }
    }

    if (context.filter_category) {
      params.set("filter_category", context.filter_category);
    }

    const { start_at_utc, end_at_utc } = timeWindowBounds(context.time_window);
    if (start_at_utc) {
      params.set("start_at_utc", start_at_utc);
    }
    if (end_at_utc) {
      params.set("end_at_utc", end_at_utc);
    }

    return params;
  }


  async function fetchBrowseRows(context) {
    const response = await fetch(`/api/browse?${buildBrowseParams(context).toString()}`);
    if (!response.ok) {
      throw new Error(`browse request failed with ${response.status}`);
    }
    const report = await response.json();
    return report.rows ?? [];
  }

  async function loadStatus() {
    const response = await fetch("/api/status");
    if (!response.ok) {
      throw new Error(`status request failed with ${response.status}`);
    }
    status = await response.json();
  }

  async function loadFilters() {
    const response = await fetch("/api/filters");
    if (!response.ok) {
      throw new Error(`filters request failed with ${response.status}`);
    }
    filters = await response.json();
  }

  async function loadBrowse({ preserveSelectionKey = null } = {}) {
    const response = await fetch(`/api/browse?${buildBrowseParams(currentContext).toString()}`);
    if (!response.ok) {
      throw new Error(`browse request failed with ${response.status}`);
    }

    browseReport = await response.json();
    const nextRows = browseReport.rows ?? [];
    childRowsByParent = {};

    if (nextRows.length === 0) {
      selectedRow = null;
      detail = null;
      return;
    }

    const preservedRow = preserveSelectionKey
      ? nextRows.find((row) => row.key === preserveSelectionKey)
      : null;
    await selectRow(preservedRow ?? nextRows[0]);
    await prefetchSunburstChildren(nextRows);
  }

  async function prefetchSunburstChildren(rows) {
    const candidates = rows
      .filter((row) => nextContextForRow(row))
      .slice(0, 10);

    if (candidates.length === 0) {
      childRowsByParent = {};
      return;
    }

    const epoch = ++prefetchEpoch;
    sunburstLoading = true;
    try {
      const loaded = await Promise.all(
        candidates.map(async (row) => {
          const nextContext = nextContextForRow(row);
          if (!nextContext) {
            return [row.key, []];
          }
          const childRows = await fetchBrowseRows(nextContext);
          return [row.key, childRows.slice(0, 8)];
        }),
      );

      if (epoch !== prefetchEpoch) {
        return;
      }

      childRowsByParent = Object.fromEntries(loaded);
    } catch (err) {
      if (epoch === prefetchEpoch) {
        childRowsByParent = {};
      }
    } finally {
      if (epoch === prefetchEpoch) {
        sunburstLoading = false;
      }
    }
  }

  async function selectRow(row) {
    if (!row) {
      selectedRow = null;
      detail = null;
      return;
    }

    selectedRow = row;
    detailError = "";
    if (!detailVisible) {
      focusBrowsePanel();
      return;
    }

    loadingDetail = true;
    try {
      const params = buildBrowseParams(currentContext);
      params.set("row_key", row.key);
      const response = await fetch(`/api/detail?${params.toString()}`);
      if (!response.ok) {
        throw new Error(`detail request failed with ${response.status}`);
      }
      detail = await response.json();
    } catch (err) {
      detail = null;
      detailError = err instanceof Error ? err.message : String(err);
    } finally {
      loadingDetail = false;
    }

    if (focusedPane === "detail") {
      focusDetailPanel();
    }
  }

  async function refreshSnapshot() {
    refreshing = true;
    error = "";
    shortcutMessage = "Refreshing pinned snapshot.";
    announce("Refreshing pinned snapshot.");
    try {
      const response = await fetch("/api/refresh", { method: "POST" });
      if (!response.ok) {
        throw new Error(`refresh request failed with ${response.status}`);
      }
      status = await response.json();
      await loadBrowse({ preserveSelectionKey: selectedRow?.key ?? null });
    } catch (err) {
      error = err instanceof Error ? err.message : String(err);
    } finally {
      refreshing = false;
    }
  }

  function formatMetric(value) {
    return new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(
      Number(value ?? 0),
    );
  }

  function formatSnapshot(snapshot) {
    if (!snapshot) return "none";
    return `#${snapshot.max_publish_seq} · ${snapshot.published_chunk_count} chunks`;
  }

  function rowHasOpportunities(row) {
    return (row.opportunities?.annotations?.length ?? 0) > 0;
  }

  function selectedRowMetric() {
    if (!selectedRow) {
      return null;
    }
    return selectedRow.metrics?.[currentContext.lens] ?? null;
  }

  function selectedRowOpportunityCount() {
    return selectedRow?.opportunities?.annotations?.length ?? 0;
  }

  function selectedRowCanDrill() {
    return Boolean(nextContextForRow(selectedRow));
  }

  function canMoveSelection(direction) {
    const rows = currentRows();
    if (rows.length === 0) {
      return false;
    }

    const index = selectedIndex();
    const baseIndex = index >= 0 ? index : 0;
    const nextIndex = Math.min(Math.max(baseIndex + direction, 0), rows.length - 1);
    return nextIndex !== index;
  }

  function chartAccessibleSummary() {
    const rows = currentRows();
    if (rows.length === 0) {
      return "No chart segments are visible for the current scope and filters.";
    }

    const summary = rows
      .slice(0, 5)
      .map((row) => {
        const metric = formatMetric(row.metrics[currentContext.lens]);
        const drill = nextContextForRow(row) ? "drill available" : "leaf";
        return `${row.label}, ${row.kind}, ${metric} ${LENS_LABELS[currentContext.lens].toLowerCase()}, ${drill}`;
      })
      .join("; ");

    return `${rows.length} visible chart segments. ${summary}`;
  }

  function currentRows() {
    let rows = browseReport?.rows ?? [];
    if (opportunityOnly) {
      rows = rows.filter((row) => rowHasOpportunities(row));
    }

    const query = rowFilter.trim().toLowerCase();
    if (!query) {
      return rows;
    }

    return rows.filter((row) => {
      const haystacks = [row.label, row.kind, row.category, row.full_path]
        .filter(Boolean)
        .map((value) => String(value).toLowerCase());
      return haystacks.some((value) => value.includes(query));
    });
  }

  function breadcrumbItems() {
    const items = contextHistory.map((context, index) => ({
      label: breadcrumbLabel(context),
      index,
    }));
    items.push({ label: breadcrumbLabel(currentContext), index: contextHistory.length });
    return items;
  }

  function chartScopeLabel() {
    return breadcrumbLabel(currentContext);
  }

  function breadcrumbLabel(context) {
    const project = context.project_id != null ? projectFilterLabel(context.project_id) : null;
    const category = context.category;
    const path = context.parent_path;
    const parts = [ROOT_LABELS[context.root]];
    if (project) parts.push(project);
    if (category) parts.push(category);
    if (path) parts.push(path);
    return parts.join(" / ");
  }

  async function jumpToQuery() {
    const query = jumpQuery.trim().toLowerCase();
    if (!query) {
      shortcutMessage = "Jump query is empty.";
      announce(shortcutMessage);
      return;
    }

    const match = (browseReport?.rows ?? []).find((row) => {
      const haystacks = [row.label, row.kind, row.category, row.full_path]
        .filter(Boolean)
        .map((value) => String(value).toLowerCase());
      return haystacks.some((value) => value.includes(query));
    });

    if (!match) {
      shortcutMessage = `No visible row matches "${jumpQuery}".`;
      announce(shortcutMessage);
      return;
    }

    await selectRow(match);
    focusedPane = "browse";
    shortcutMessage = `Jumped to ${match.label}.`;
    announce(shortcutMessage);
  }

  function toggleBreadcrumbs() {
    showBreadcrumbs = !showBreadcrumbs;
    shortcutMessage = showBreadcrumbs ? "Breadcrumbs shown." : "Breadcrumbs hidden.";
    announce(shortcutMessage);
  }

  function focusRowFilter() {
    rowFilterInput?.focus();
    rowFilterInput?.select();
    shortcutMessage = "Row filter focused.";
    announce(shortcutMessage);
  }

  function focusJumpField() {
    jumpInput?.focus();
    jumpInput?.select();
    shortcutMessage = "Jump field focused.";
    announce(shortcutMessage);
  }

  function toggleOpportunityOnly() {
    opportunityOnly = !opportunityOnly;
    shortcutMessage = opportunityOnly
      ? "Showing only rows with opportunity annotations."
      : "Showing all rows again.";
    announce(shortcutMessage);
  }

  function toggleRowDensity() {
    compactRows = !compactRows;
    shortcutMessage = compactRows ? "Compact row layout enabled." : "Expanded row layout enabled.";
    announce(shortcutMessage);
  }

  function selectedIndex() {
    return currentRows().findIndex((row) => row.key === selectedRow?.key);
  }

  async function moveSelection(direction) {
    const rows = currentRows();
    if (rows.length === 0) {
      shortcutMessage = "No rows available in this browse view.";
      announce(shortcutMessage);
      return;
    }

    const index = selectedIndex();
    const baseIndex = index >= 0 ? index : 0;
    const nextIndex = Math.min(Math.max(baseIndex + direction, 0), rows.length - 1);
    if (nextIndex === index) {
      return;
    }

    await selectRow(rows[nextIndex]);
    shortcutMessage = `Selected ${rows[nextIndex].label}.`;
    announce(shortcutMessage);
  }

  function cycleList(values, current, direction = 1) {
    const index = values.indexOf(current);
    const baseIndex = index >= 0 ? index : 0;
    const nextIndex = (baseIndex + direction + values.length) % values.length;
    return values[nextIndex];
  }

  async function setRoot(root) {
    currentContext = {
      ...createRootContext(root, currentContext.lens),
      model: currentContext.model,
      filter_project_id: currentContext.filter_project_id,
      filter_category: currentContext.filter_category,
      time_window: currentContext.time_window,
    };
    contextHistory = [];
    focusedPane = "browse";
    error = "";
    detailError = "";
    await loadBrowse();
    shortcutMessage = `Switched to ${ROOT_LABELS[root]}.`;
    announce(shortcutMessage);
  }

  async function cycleLens() {
    currentContext = {
      ...currentContext,
      lens: cycleList(LENS_OPTIONS, currentContext.lens),
    };
    await loadBrowse({ preserveSelectionKey: selectedRow?.key ?? null });
    shortcutMessage = `Lens: ${LENS_LABELS[currentContext.lens]}.`;
    announce(shortcutMessage);
  }

  function nextContextForRow(row) {
    if (!row) {
      return null;
    }

    if (currentContext.root === "project") {
      if (row.kind === "Project" && row.project_id != null) {
        return {
          ...currentContext,
          path: "project",
          project_id: row.project_id,
          category: null,
          classification_state: null,
          normalized_action: null,
          command_family: null,
          base_command: null,
          parent_path: null,
        };
      }

      if (row.kind === "ActionCategory" && currentContext.project_id != null && row.category) {
        return {
          ...currentContext,
          path: "project_category",
          project_id: currentContext.project_id,
          category: row.category,
          classification_state: null,
          normalized_action: null,
          command_family: null,
          base_command: null,
          parent_path: null,
        };
      }

      if (
        row.kind === "Action" &&
        currentContext.project_id != null &&
        row.category &&
        row.action
      ) {
        return {
          ...currentContext,
          path: "project_action",
          project_id: currentContext.project_id,
          category: row.category,
          classification_state: row.action.classification_state,
          normalized_action: row.action.normalized_action,
          command_family: row.action.command_family,
          base_command: row.action.base_command,
          parent_path: null,
        };
      }

      if (row.kind === "Directory" && currentContext.path === "project_action") {
        return {
          ...currentContext,
          parent_path: row.full_path,
        };
      }

      return null;
    }

    if (row.kind === "ActionCategory" && row.category) {
      return {
        ...currentContext,
        path: "category",
        project_id: null,
        category: row.category,
        classification_state: null,
        normalized_action: null,
        command_family: null,
        base_command: null,
        parent_path: null,
      };
    }

    if (row.kind === "Action" && currentContext.category && row.action) {
      return {
        ...currentContext,
        path: "category_action",
        project_id: null,
        category: currentContext.category,
        classification_state: row.action.classification_state,
        normalized_action: row.action.normalized_action,
        command_family: row.action.command_family,
        base_command: row.action.base_command,
        parent_path: null,
      };
    }

    if (
      row.kind === "Project" &&
      currentContext.path === "category_action" &&
      row.project_id != null
    ) {
      return {
        ...currentContext,
        path: "category_action_project",
        project_id: row.project_id,
        parent_path: null,
      };
    }

    if (row.kind === "Directory" && currentContext.path === "category_action_project") {
      return {
        ...currentContext,
        parent_path: row.full_path,
      };
    }

    return null;
  }

  async function drillIntoRow(row) {
    const nextContext = nextContextForRow(row);
    if (!nextContext) {
      shortcutMessage = "Selected row has no deeper browse level.";
      announce(shortcutMessage);
      return;
    }

    selectedRow = row;
    contextHistory = [...contextHistory, copyContext(currentContext)];
    currentContext = nextContext;
    focusedPane = "browse";
    await loadBrowse();
    shortcutMessage = `Drilled into ${row.label}.`;
    announce(shortcutMessage);
  }

  async function drillIntoSelected() {
    await drillIntoRow(selectedRow);
  }

  async function navigateUp() {
    if (contextHistory.length === 0) {
      shortcutMessage = "Already at the root browse view.";
      announce(shortcutMessage);
      return;
    }

    const previous = contextHistory[contextHistory.length - 1];
    contextHistory = contextHistory.slice(0, -1);
    currentContext = previous;
    focusedPane = "browse";
    await loadBrowse();
    shortcutMessage = "Moved to parent scope.";
    announce(shortcutMessage);
  }

  async function clearScope() {
    if (currentContext.path === "root" && contextHistory.length === 0) {
      shortcutMessage = "Already at the unscoped root view.";
      announce(shortcutMessage);
      return;
    }

    currentContext = {
      ...createRootContext(currentContext.root, currentContext.lens),
      model: currentContext.model,
      filter_project_id: currentContext.filter_project_id,
      filter_category: currentContext.filter_category,
      time_window: currentContext.time_window,
    };
    contextHistory = [];
    focusedPane = "browse";
    await loadBrowse();
    shortcutMessage = "Cleared scope back to the root view.";
    announce(shortcutMessage);
  }

  async function clearFilters() {
    currentContext = {
      ...currentContext,
      model: null,
      filter_project_id: null,
      filter_category: null,
      time_window: "all",
    };
    await loadBrowse({ preserveSelectionKey: selectedRow?.key ?? null });
    shortcutMessage = "Cleared active filters.";
    announce(shortcutMessage);
  }

  function togglePaneFocus() {
    focusedPane = focusedPane === "browse" ? "detail" : "browse";
    shortcutMessage = focusedPane === "browse" ? "Browse pane focused." : "Detail pane focused.";
    announce(shortcutMessage);
    if (focusedPane === "browse") {
      focusBrowsePanel();
    } else if (detailVisible) {
      focusDetailPanel();
    }
  }

  async function toggleDetailPane() {
    detailVisible = !detailVisible;
    if (detailVisible && selectedRow && !detail) {
      await selectRow(selectedRow);
    }
    if (!detailVisible) {
      focusedPane = "browse";
    }
    shortcutMessage = detailVisible ? "Detail pane opened." : "Detail pane hidden.";
    announce(shortcutMessage);
    if (detailVisible) {
      focusDetailPanel();
    } else {
      focusBrowsePanel();
    }
  }

  async function cycleTimeWindow() {
    currentContext = {
      ...currentContext,
      time_window: cycleList(
        TIME_WINDOW_OPTIONS.map((option) => option.key),
        currentContext.time_window,
      ),
    };
    await loadBrowse({ preserveSelectionKey: selectedRow?.key ?? null });
    shortcutMessage = `Time window: ${timeWindowLabel(currentContext.time_window)}.`;
    announce(shortcutMessage);
  }

  async function cycleModel() {
    const values = [null, ...(filters?.models ?? [])];
    if (values.length <= 1) {
      shortcutMessage = "No model filters available yet.";
      announce(shortcutMessage);
      return;
    }

    currentContext = {
      ...currentContext,
      model: cycleNullableList(values, currentContext.model),
    };
    await loadBrowse({ preserveSelectionKey: selectedRow?.key ?? null });
    shortcutMessage = `Model filter: ${currentContext.model ?? "all models"}.`;
    announce(shortcutMessage);
  }

  async function cycleProjectFilter() {
    if (currentContext.path !== "root") {
      shortcutMessage = "Project filter is only available from the root view right now.";
      announce(shortcutMessage);
      return;
    }

    const values = [null, ...(filters?.projects?.map((project) => project.id) ?? [])];
    if (values.length <= 1) {
      shortcutMessage = "No project filters available yet.";
      announce(shortcutMessage);
      return;
    }

    currentContext = {
      ...currentContext,
      filter_project_id: cycleNullableList(values, currentContext.filter_project_id),
    };
    await loadBrowse();
    shortcutMessage = `Project filter: ${projectFilterLabel(currentContext.filter_project_id)}.`;
    announce(shortcutMessage);
  }

  async function cycleCategoryFilter() {
    const values = [null, ...(filters?.categories ?? [])];
    if (values.length <= 1) {
      shortcutMessage = "No category filters available yet.";
      announce(shortcutMessage);
      return;
    }

    currentContext = {
      ...currentContext,
      filter_category: cycleNullableList(values, currentContext.filter_category),
    };
    await loadBrowse({ preserveSelectionKey: selectedRow?.key ?? null });
    shortcutMessage = `Category filter: ${currentContext.filter_category ?? "all categories"}.`;
    announce(shortcutMessage);
  }

  function cycleNullableList(values, current) {
    const index = values.findIndex((value) => value === current);
    const baseIndex = index >= 0 ? index : 0;
    return values[(baseIndex + 1) % values.length];
  }

  async function updateModel(event) {
    currentContext = {
      ...currentContext,
      model: event.currentTarget.value || null,
    };
    await loadBrowse({ preserveSelectionKey: selectedRow?.key ?? null });
  }

  async function updateProjectFilter(event) {
    currentContext = {
      ...currentContext,
      filter_project_id: event.currentTarget.value ? Number(event.currentTarget.value) : null,
    };
    await loadBrowse();
  }

  async function updateCategoryFilter(event) {
    currentContext = {
      ...currentContext,
      filter_category: event.currentTarget.value || null,
    };
    await loadBrowse({ preserveSelectionKey: selectedRow?.key ?? null });
  }

  async function updateTimeWindow(event) {
    currentContext = {
      ...currentContext,
      time_window: event.currentTarget.value,
    };
    await loadBrowse({ preserveSelectionKey: selectedRow?.key ?? null });
  }

  function projectFilterLabel(projectId) {
    if (projectId == null) {
      return "all projects";
    }
    return filters?.projects?.find((project) => project.id === projectId)?.display_name ?? `project ${projectId}`;
  }

  function timeWindowLabel(key) {
    return TIME_WINDOW_OPTIONS.find((option) => option.key === key)?.label ?? "All time";
  }

  function isEditableTarget(target) {
    if (!(target instanceof HTMLElement)) {
      return false;
    }

    return (
      target.isContentEditable ||
      ["INPUT", "TEXTAREA", "SELECT"].includes(target.tagName)
    );
  }

  async function handleKeydown(event) {
    if (event.metaKey || event.ctrlKey || event.altKey || isEditableTarget(event.target)) {
      return;
    }

    if (event.key === "/") {
      event.preventDefault();
      focusRowFilter();
      return;
    }

    if (event.key === "g") {
      event.preventDefault();
      focusJumpField();
      return;
    }

    if (event.key === "b") {
      event.preventDefault();
      toggleBreadcrumbs();
      return;
    }

    if (event.key === "Tab") {
      event.preventDefault();
      togglePaneFocus();
      return;
    }

    if (event.key === "i") {
      event.preventDefault();
      await toggleDetailPane();
      return;
    }

    if (event.key === "r") {
      event.preventDefault();
      await refreshSnapshot();
      return;
    }

    if (event.key === "l") {
      event.preventDefault();
      await cycleLens();
      return;
    }

    if (event.key === "1") {
      event.preventDefault();
      await setRoot("project");
      return;
    }

    if (event.key === "2") {
      event.preventDefault();
      await setRoot("category");
      return;
    }

    if (event.key === "t") {
      event.preventDefault();
      await cycleTimeWindow();
      return;
    }

    if (event.key === "m") {
      event.preventDefault();
      await cycleModel();
      return;
    }

    if (event.key === "p") {
      event.preventDefault();
      await cycleProjectFilter();
      return;
    }

    if (event.key === "c") {
      event.preventDefault();
      await cycleCategoryFilter();
      return;
    }

    if (event.key === "0") {
      event.preventDefault();
      await clearFilters();
      return;
    }

    if (event.key === " ") {
      event.preventDefault();
      await clearScope();
      return;
    }

    if (event.key === "ArrowDown" || event.key === "j") {
      event.preventDefault();
      await moveSelection(1);
      return;
    }

    if (event.key === "ArrowUp" || event.key === "k") {
      event.preventDefault();
      await moveSelection(-1);
      return;
    }

    if (event.key === "Enter" || event.key === "ArrowRight") {
      event.preventDefault();
      await drillIntoSelected();
      return;
    }

    if (event.key === "Backspace" || event.key === "ArrowLeft") {
      event.preventDefault();
      await navigateUp();
      return;
    }

    if (event.key === "x") {
      event.preventDefault();
      toggleOpportunityOnly();
      return;
    }

    if (event.key === "o") {
      event.preventDefault();
      toggleRowDensity();
    }
  }

  onMount(async () => {
    try {
      await Promise.all([loadStatus(), loadFilters()]);
      await loadBrowse();
    } catch (err) {
      error = err instanceof Error ? err.message : String(err);
    } finally {
      loading = false;
    }
  });
</script>

<svelte:head>
  <title>gnomon-web</title>
</svelte:head>

<svelte:window on:keydown={handleKeydown} />

{#if loading}
  <main class="loading" aria-busy="true">Loading gnomon-web...</main>
{:else}
  <main class="app-shell">
    <div class="sr-only" aria-live="polite" aria-atomic="true">{liveMessage}</div>

    <header class="hero" aria-labelledby="app-title">
      <div>
        <p class="eyebrow">gnomon-web</p>
        <h1 id="app-title">Browser shell bootstrap</h1>
        <p class="lede">
          The browser shell now talks to the local <code>gnomon-web</code> backend and renders
          status, sunburst, browse, detail, and filter surfaces in the DOM.
        </p>
      </div>
      <div class="hero-actions">
        <button class="refresh" on:click={refreshSnapshot} disabled={refreshing}>
          {#if refreshing}Refreshing...{:else}Refresh snapshot{/if}
        </button>
      </div>
    </header>

    {#if error}
      <section class="banner error" role="alert">{error}</section>
    {/if}

    <section class="status-grid" aria-label="Snapshot status overview">
      <article class="status-card">
        <p class="label">Pinned snapshot</p>
        <strong>{formatSnapshot(status?.pinned_snapshot)}</strong>
      </article>
      <article class="status-card">
        <p class="label">Latest snapshot</p>
        <strong>{formatSnapshot(status?.latest_snapshot)}</strong>
      </article>
      <article class="status-card">
        <p class="label">Coverage</p>
        <strong>{status?.coverage?.project_count ?? 0} projects</strong>
        <span>{status?.coverage?.turn_count ?? 0} turns</span>
      </article>
      <article class="status-card">
        <p class="label">Asset mode</p>
        <strong>{status?.using_built_assets ? "built bundle" : "fallback bundle"}</strong>
        <span>{status?.has_newer_snapshot ? "newer snapshot available" : "up to date"}</span>
      </article>
    </section>

    <nav class="toolbar" aria-label="Primary browser controls">
      <div class="toolbar-group" role="group" aria-label="Root hierarchy">
        {#each ROOT_OPTIONS as root}
          <button
            class:active={currentContext.root === root}
            on:click={() => setRoot(root)}
            type="button"
          >
            {root === "project" ? "1 Project" : "2 Category"}
          </button>
        {/each}
      </div>
      <div class="toolbar-group" role="group" aria-label="Metric lens and shell controls">
        <button type="button" class="pill" on:click={cycleLens}>
          L Lens: {LENS_LABELS[currentContext.lens]}
        </button>
        <button type="button" class="pill" on:click={clearScope}>
          Space Clear scope
        </button>
        <button type="button" class="pill" on:click={clearFilters}>
          0 Clear filters
        </button>
        <button type="button" class="pill" on:click={toggleDetailPane}>
          I {detailVisible ? "Hide" : "Show"} detail
        </button>
        <button type="button" class:active={opportunityOnly} class="pill" on:click={toggleOpportunityOnly}>
          X Opportunity only
        </button>
        <button type="button" class:active={compactRows} class="pill" on:click={toggleRowDensity}>
          O {compactRows ? "Expanded" : "Compact"} rows
        </button>
      </div>
    </nav>

    <section class="filter-bar" aria-label="Filters">
      <label>
        <span>T time</span>
        <select on:change={updateTimeWindow} bind:value={currentContext.time_window}>
          {#each TIME_WINDOW_OPTIONS as option}
            <option value={option.key}>{option.label}</option>
          {/each}
        </select>
      </label>
      <label>
        <span>M model</span>
        <select on:change={updateModel} value={currentContext.model ?? ""}>
          <option value="">All models</option>
          {#each filters?.models ?? [] as model}
            <option value={model}>{model}</option>
          {/each}
        </select>
      </label>
      <label>
        <span>P project</span>
        <select
          on:change={updateProjectFilter}
          bind:value={projectFilterValue}
          disabled={currentContext.path !== "root"}
        >
          <option value="">All projects</option>
          {#each filters?.projects ?? [] as project}
            <option value={project.id}>{project.display_name}</option>
          {/each}
        </select>
      </label>
      <label>
        <span>C category</span>
        <select on:change={updateCategoryFilter} bind:value={categoryFilterValue}>
          <option value="">All categories</option>
          {#each filters?.categories ?? [] as category}
            <option value={category}>{category}</option>
          {/each}
        </select>
      </label>
    </section>


    <section class="aux-bar" aria-label="Search and jump controls">
      <label>
        <span>/ row filter</span>
        <input bind:this={rowFilterInput} bind:value={rowFilter} type="search" placeholder="Filter visible rows" />
      </label>
      <label>
        <span>g jump</span>
        <div class="jump-field">
          <input bind:this={jumpInput} bind:value={jumpQuery} type="search" placeholder="Jump to visible row" />
          <button type="button" on:click={jumpToQuery}>Jump</button>
        </div>
      </label>
    </section>

    <section class="status-strip" aria-label="Current browser state">
      <p>
        <strong>Scope</strong>
        <span>{ROOT_LABELS[currentContext.root]}</span>
      </p>
      <p>
        <strong>Path</strong>
        <span>{currentContext.path}</span>
      </p>
      <p>
        <strong>Focus</strong>
        <span>{focusedPane}</span>
      </p>
      <p>
        <strong>Filters</strong>
        <span>
          {timeWindowLabel(currentContext.time_window)} · {currentContext.model ?? "all models"} ·
          {projectFilterLabel(currentContext.filter_project_id)} · {currentContext.filter_category ?? "all categories"} · {opportunityOnly ? "opportunities only" : "all rows"}
        </span>
      </p>
    </section>

    {#if showBreadcrumbs}
      <nav class="banner breadcrumb-banner" aria-label="Breadcrumbs">
        <strong>Breadcrumbs</strong>
        {#each breadcrumbItems() as crumb, index}
          <button
            type="button"
            class:active={index === contextHistory.length}
            on:click={async () => {
              if (index === contextHistory.length) return;
              currentContext = copyContext(contextHistory[index]);
              contextHistory = contextHistory.slice(0, index);
              focusedPane = "browse";
              await loadBrowse();
              shortcutMessage = `Jumped to ${crumb.label}.`;
              announce(shortcutMessage);
            }}
          >
            {crumb.label}
          </button>
        {/each}
      </nav>
    {/if}

    <section class="banner shortcut-banner" aria-label="Keyboard shortcuts">
      <strong>Keyboard</strong>
      <span>1/2 root</span>
      <span>l lens</span>
      <span>t time</span>
      <span>m model</span>
      <span>p project</span>
      <span>c category</span>
      <span>j/k or arrows move</span>
      <span>Enter or → drill</span>
      <span>Backspace or ← up</span>
      <span>Tab focus</span>
      <span>i detail</span>
      <span>r refresh</span>
      <span>Space clear scope</span>
      <span>0 clear filters</span>
      <span>x opportunities</span>
      <span>o row density</span>
      {#if shortcutMessage}
        <span class="shortcut-message">{shortcutMessage}</span>
      {/if}
    </section>

    <section class="workspace">
      <section
        class:focused-pane={focusedPane === "browse"}
        class="panel browse-panel"
        aria-labelledby="browse-heading"
        tabindex="-1"
        bind:this={browsePanel}
      >
        <header class="panel-header browse-header">
          <div>
            <p class="eyebrow">Map</p>
            <h2 id="browse-heading">{ROOT_LABELS[currentContext.root]}</h2>
            <p class="chart-caption">{chartScopeLabel()} · {LENS_LABELS[currentContext.lens]}</p>
          </div>
          <span>{currentRows().length} rows{#if sunburstLoading} · mapping deeper rings{/if}</span>
        </header>

        <div class="browse-chart-wrap">
          <SunburstChart
            rows={currentRows()}
            lens={currentContext.lens}
            lensLabel={LENS_LABELS[currentContext.lens]}
            rootLabel={chartScopeLabel()}
            selectedKey={selectedRow?.key ?? null}
            selectedRow={selectedRow}
            childRowsByParent={childRowsByParent}
            loading={sunburstLoading}
            accessibleLabel={`${ROOT_LABELS[currentContext.root]} sunburst for ${chartScopeLabel()}`}
            accessibleDescriptionId="chart-accessibility-summary"
            on:select={(event) => selectRow(event.detail.row)}
            on:drill={(event) => drillIntoRow(event.detail.row)}
          />

          <div class="chart-summary" aria-live="polite">
            <div>
              <strong>Chart selection</strong>
              <p id="chart-accessibility-summary" class="chart-instructions">
                {chartAccessibleSummary()} Use previous and next segment controls, the browse list,
                or keyboard shortcuts to move selection without the pointer.
              </p>
              {#if selectedRow}
                <p>
                  {selectedRow.label} · {selectedRow.kind} · {formatMetric(selectedRowMetric())}
                  {LENS_LABELS[currentContext.lens].toLowerCase()}
                </p>
                <p>
                  {selectedRowOpportunityCount()} opportunity annotation{selectedRowOpportunityCount() === 1 ? "" : "s"}
                  {#if selectedRowCanDrill()} · deeper scope available{/if}
                </p>
              {:else}
                <p>No row selected. Use the keyboard or chart to choose a segment.</p>
              {/if}
            </div>

            <div class="chart-summary-actions">
              <button
                type="button"
                on:click={() => moveSelection(-1)}
                disabled={!canMoveSelection(-1)}
              >
                Previous segment
              </button>
              <button
                type="button"
                on:click={() => moveSelection(1)}
                disabled={!canMoveSelection(1)}
              >
                Next segment
              </button>
              <button
                type="button"
                on:click={() => drillIntoSelected()}
                disabled={!selectedRowCanDrill()}
              >
                Drill into selection
              </button>
              <button
                type="button"
                on:click={() => navigateUp()}
                disabled={contextHistory.length === 0}
              >
                Move to parent scope
              </button>
              <button type="button" on:click={focusBrowsePanel}>
                Focus browse pane
              </button>
            </div>
          </div>
        </div>

        <div class="browse-list-block">
          <div class="subpanel-header">
            <div>
              <p class="eyebrow">Rows</p>
              <p class="subpanel-note">List view mirrors the current chart scope and selection.</p>
            </div>
            <span>{compactRows ? "compact" : "expanded"}{#if sunburstLoading} · loading child rings{/if}</span>
          </div>
          {#if currentRows().length}
            <ul class:compact={compactRows} class="row-list" role="listbox" aria-label="Browse rows">
              {#each currentRows() as row}
                <li>
                  <button
                    class:selected={selectedRow?.key === row.key}
                    on:click={() => selectRow(row)}
                    type="button"
                    aria-pressed={selectedRow?.key === row.key}
                    aria-current={selectedRow?.key === row.key ? "true" : undefined}
                  >
                    <span class="row-title">{row.label}</span>
                    <span class="row-meta">
                      {formatMetric(row.metrics[currentContext.lens])} {LENS_LABELS[currentContext.lens].toLowerCase()}
                    </span>
                    <span class="row-tags">{row.kind}{#if row.category} · {row.category}{/if}</span>
                    {#if rowHasOpportunities(row)}
                      <span class="row-badge">{row.opportunities.annotations.length} opportunity{row.opportunities.annotations.length === 1 ? "" : "ies"}</span>
                    {/if}
                    {#if !compactRows && row.full_path}
                      <span class="row-path">{row.full_path}</span>
                    {/if}
                  </button>
                </li>
              {/each}
            </ul>
          {:else}
            <p class="empty-state">No visible rows yet for the pinned snapshot, current filters, and opportunity mode.</p>
          {/if}
        </div>
      </section>

      {#if detailVisible}
        <section
          class:focused-pane={focusedPane === "detail"}
          class="panel detail-panel"
          aria-labelledby="detail-heading"
          tabindex="-1"
          bind:this={detailPanel}
        >
          <header class="panel-header">
            <div>
              <p class="eyebrow">Detail</p>
              <h2 id="detail-heading">{selectedRow?.label ?? "Select a row"}</h2>
            </div>
          </header>

          {#if loadingDetail}
            <p class="empty-state">Loading detail...</p>
          {:else if detailError}
            <p class="empty-state error">{detailError}</p>
          {:else if detail}
            <dl class="detail-grid">
              <div>
                <dt>Kind</dt>
                <dd>{detail.row.kind}</dd>
              </div>
              <div>
                <dt>Items</dt>
                <dd>{detail.row.item_count}</dd>
              </div>
              <div>
                <dt>Uncached input</dt>
                <dd>{formatMetric(detail.row.metrics.uncached_input)}</dd>
              </div>
              <div>
                <dt>Gross input</dt>
                <dd>{formatMetric(detail.row.metrics.gross_input)}</dd>
              </div>
              <div>
                <dt>Output</dt>
                <dd>{formatMetric(detail.row.metrics.output)}</dd>
              </div>
              <div>
                <dt>Path</dt>
                <dd>{detail.row.full_path ?? "-"}</dd>
              </div>
            </dl>

            <section class="opportunities">
              <h3>Opportunities</h3>
              {#if detail.row.opportunities?.annotations?.length}
                <ul>
                  {#each detail.row.opportunities.annotations as annotation}
                    <li>
                      <strong>{annotation.category}</strong>
                      <span>{annotation.confidence}</span>
                      <p>{annotation.recommendation ?? "No recommendation text yet."}</p>
                    </li>
                  {/each}
                </ul>
              {:else}
                <p class="empty-state">No confident opportunity annotations for this row.</p>
              {/if}
            </section>
          {:else}
            <p class="empty-state">Select a browse row to inspect its metrics and annotations.</p>
          {/if}
        </section>
      {/if}
    </section>
  </main>
{/if}
