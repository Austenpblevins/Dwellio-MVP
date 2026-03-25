import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Dwellio Parcel Search",
  description: "Public parcel summary and tax-data lookup for Texas homeowners.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
