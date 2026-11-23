import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { ProgressBar, RefreshButton } from "./ui";

describe("RefreshButton", () => {
  it("has an accessible name", () => {
    // It is icon-only, so without this a screen reader announces "button".
    render(<RefreshButton onClick={() => {}} />);

    expect(screen.getByRole("button", { name: "Refresh" })).toBeInTheDocument();
  });

  it("takes a caller-supplied label", () => {
    render(<RefreshButton onClick={() => {}} label="Refresh graphs" />);

    expect(
      screen.getByRole("button", { name: "Refresh graphs" }),
    ).toBeInTheDocument();
  });

  it("still invokes its handler", async () => {
    const onClick = vi.fn();
    render(<RefreshButton onClick={onClick} />);

    await userEvent.click(screen.getByRole("button", { name: "Refresh" }));

    expect(onClick).toHaveBeenCalledOnce();
  });
});

describe("ProgressBar", () => {
  it("shows the label and no percentage text in its default form", () => {
    render(<ProgressBar value={42} label="Running Athena query" />);

    expect(screen.getByText("Running Athena query")).toBeInTheDocument();
    expect(screen.queryByText("42%")).not.toBeInTheDocument();
  });

  it("shows the percentage in its compact form", () => {
    // The table-cell form the Projections page uses: a short bar plus the
    // number, where a full-width bar would blow out the column.
    render(<ProgressBar value={42} label="Importing" compact />);

    expect(screen.getByText("42%")).toBeInTheDocument();
  });

  it("rounds the percentage rather than printing a float", () => {
    render(<ProgressBar value={42.6} compact />);

    expect(screen.getByText("43%")).toBeInTheDocument();
  });

  it("keeps the label reachable as a title in compact form", () => {
    // The compact form has no room for the step label as text, so it must not
    // be dropped entirely — that was the regression in the Projections table.
    const { container } = render(
      <ProgressBar value={10} label="Creating graph" compact />,
    );

    expect(container.querySelector('[title="Creating graph"]')).not.toBeNull();
  });

  it("caps the fill at 100% when handed more", () => {
    const { container } = render(<ProgressBar value={150} />);

    const fill = container.querySelector<HTMLElement>("[style]");
    expect(fill?.style.width).toBe("100%");
  });
});
