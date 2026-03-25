import type { Metadata } from "next";
import { Suspense } from "react";
import "./globals.css";
import { AttributionTracker } from "./_components/AttributionTracker";

export const metadata: Metadata = {
  title: "Dwellio Property Tax Protest Quotes",
  description:
    "Search a Texas parcel, review a quote-safe protest signal, and leave your email for the next step.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>
        <Suspense fallback={null}>
          <AttributionTracker />
        </Suspense>
        {children}
      </body>
    </html>
  );
}
