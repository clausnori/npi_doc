"use client"

import { useState, useEffect } from "react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { CheckCircle, XCircle, AlertCircle, RefreshCw } from "lucide-react"

export function ApiStatus() {
  const [status, setStatus] = useState<"checking" | "connected" | "error" | "timeout">("checking")
  const [error, setError] = useState<string | null>(null)
  const [lastChecked, setLastChecked] = useState<Date | null>(null)

  const checkApiStatus = async () => {
    setStatus("checking")
    setError(null)

    try {
      const controller = new AbortController()
      const timeoutId = setTimeout(() => controller.abort(), 5000)

      const response = await fetch("/api/doctors?page=1&limit=1", {
        signal: controller.signal,
      })

      clearTimeout(timeoutId)

      if (response.ok) {
        const data = await response.json()
        if (data.error) {
          setStatus("error")
          setError(data.message || data.details || "Unknown API error")
        } else {
          setStatus("connected")
        }
      } else {
        setStatus("error")
        const errorData = await response.json().catch(() => ({}))
        setError(errorData.message || `HTTP ${response.status}`)
      }
    } catch (err) {
      if (err instanceof Error && err.name === "AbortError") {
        setStatus("timeout")
        setError("Request timed out")
      } else {
        setStatus("error")
        setError(err instanceof Error ? err.message : "Connection failed")
      }
    }

    setLastChecked(new Date())
  }

  useEffect(() => {
    checkApiStatus()
  }, [])

  const getStatusIcon = () => {
    switch (status) {
      case "checking":
        return <RefreshCw className="w-4 h-4 animate-spin text-blue-500" />
      case "connected":
        return <CheckCircle className="w-4 h-4 text-green-500" />
      case "timeout":
        return <AlertCircle className="w-4 h-4 text-yellow-500" />
      case "error":
        return <XCircle className="w-4 h-4 text-red-500" />
    }
  }

  const getStatusBadge = () => {
    switch (status) {
      case "checking":
        return <Badge variant="secondary">Проверка...</Badge>
      case "connected":
        return <Badge className="bg-green-100 text-green-800">Подключено</Badge>
      case "timeout":
        return <Badge variant="destructive">Таймаут</Badge>
      case "error":
        return <Badge variant="destructive">Ошибка</Badge>
    }
  }

  return (
    <Card className="bg-white/60 backdrop-blur-sm border-white/20 shadow-lg">
      <CardHeader className="pb-3">
        <CardTitle className="text-lg flex items-center space-x-2">
          {getStatusIcon()}
          <span>Статус API</span>
          {getStatusBadge()}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        <div className="text-sm text-gray-600">
          <p>
            <strong>Endpoint:</strong> http://127.0.0.1:8000/providers/
          </p>
          {lastChecked && (
            <p>
              <strong>Последняя проверка:</strong> {lastChecked.toLocaleTimeString("ru-RU")}
            </p>
          )}
        </div>

        {error && (
          <div className="p-3 bg-red-50 border border-red-200 rounded-md">
            <p className="text-sm text-red-800 font-medium">Ошибка:</p>
            <p className="text-sm text-red-700 mt-1">{error}</p>
          </div>
        )}

        {status === "error" && (
          <div className="p-3 bg-blue-50 border border-blue-200 rounded-md">
            <p className="text-sm text-blue-800 font-medium">Возможные решения:</p>
            <ul className="text-sm text-blue-700 mt-1 list-disc list-inside space-y-1">
              <li>Убедитесь, что API сервер запущен на http://127.0.0.1:8000</li>
              <li>Проверьте, что endpoint /providers/ доступен</li>
              <li>Проверьте CORS настройки на сервере</li>
              <li>Убедитесь, что сервер возвращает JSON</li>
            </ul>
          </div>
        )}

        <Button
          onClick={checkApiStatus}
          variant="outline"
          size="sm"
          disabled={status === "checking"}
          className="w-full bg-transparent"
        >
          {status === "checking" ? "Проверка..." : "Проверить снова"}
        </Button>
      </CardContent>
    </Card>
  )
}
