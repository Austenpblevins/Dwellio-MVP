import Link from "next/link";

const ADMIN_LINKS = [
  { href: "/admin/ops", label: "Overview" },
  { href: "/admin/cases", label: "Cases" },
  { href: "/admin/packets", label: "Packets" },
  { href: "/admin/ops/jobs", label: "Jobs" },
  { href: "/admin/ops/validation", label: "Validation" },
  { href: "/admin/ops/source-files", label: "Source Files" },
  { href: "/admin/ops/manual-upload", label: "Manual Upload" },
  { href: "/admin/ops/completeness", label: "Completeness" },
  { href: "/admin/ops/tax-assignment", label: "Tax Assignment" },
  { href: "/admin/readiness", label: "Readiness" },
  { href: "/admin/leads", label: "Leads" },
];

export function AdminOpsNav() {
  return (
    <nav className="flex flex-wrap gap-3">
      {ADMIN_LINKS.map((link) => (
        <Link
          key={link.href}
          href={link.href}
          className="rounded-full border border-slate-300 bg-white px-4 py-2 text-sm font-semibold text-slate-700 transition hover:border-slate-950 hover:text-slate-950"
        >
          {link.label}
        </Link>
      ))}
    </nav>
  );
}
