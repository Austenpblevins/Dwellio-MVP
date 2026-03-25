import { cookies } from "next/headers";
import { redirect } from "next/navigation";

export function getExpectedAdminToken(): string {
  return process.env.DWELLIO_ADMIN_API_TOKEN ?? "dev-admin-token";
}

export async function requireAdminToken(): Promise<string> {
  const cookieStore = await cookies();
  const token = cookieStore.get("dwellio_admin_token")?.value;

  if (token !== getExpectedAdminToken()) {
    redirect("/admin/login");
  }

  return token;
}

export async function setAdminTokenCookie(token: string): Promise<void> {
  const cookieStore = await cookies();
  cookieStore.set("dwellio_admin_token", token, {
    httpOnly: true,
    sameSite: "lax",
    path: "/",
  });
}

export async function clearAdminTokenCookie(): Promise<void> {
  const cookieStore = await cookies();
  cookieStore.delete("dwellio_admin_token");
}
