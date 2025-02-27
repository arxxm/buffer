package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Константы для API
const (
	apiURL    = "https://development.kpi-drive.ru/_api/facts/save_fact"
	authToken = "48ab34464a5573519725deb5865cc74c"
)

// Fact — структура записи, отправляемой на сервер
type Fact struct {
	PeriodStart         string
	PeriodEnd           string
	PeriodKey           string
	IndicatorToMoID     int
	IndicatorToMoFactID int
	Value               int
	FactTime            string
	IsPlan              int
	AuthUserID          int
	Comment             string
}

// sendFact — отправка одной записи на сервер
func sendFact(ctx context.Context, fact Fact) error {
	// Формируем данные в формате x-www-form-urlencoded
	data := url.Values{}
	data.Set("period_start", fact.PeriodStart)
	data.Set("period_end", fact.PeriodEnd)
	data.Set("period_key", fact.PeriodKey)
	data.Set("indicator_to_mo_id", fmt.Sprintf("%d", fact.IndicatorToMoID))
	data.Set("indicator_to_mo_fact_id", fmt.Sprintf("%d", fact.IndicatorToMoFactID))
	data.Set("value", fmt.Sprintf("%d", fact.Value))
	data.Set("fact_time", fact.FactTime)
	data.Set("is_plan", fmt.Sprintf("%d", fact.IsPlan))
	data.Set("auth_user_id", fmt.Sprintf("%d", fact.AuthUserID))
	data.Set("comment", fact.Comment)

	// Создаем HTTP-запрос
	req, err := http.NewRequestWithContext(ctx, "POST", apiURL, bytes.NewBufferString(data.Encode()))
	if err != nil {
		return fmt.Errorf("ошибка создания запроса: %w", err)
	}

	// Устанавливаем заголовки
	req.Header.Set("Authorization", "Bearer "+authToken)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	// Отправляем запрос
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("ошибка отправки запроса: %w", err)
	}
	defer resp.Body.Close()

	// Читаем ответ сервера
	body, _ := ioutil.ReadAll(resp.Body)
	log.Printf("Ответ сервера: %d %s", resp.StatusCode, string(body))

	// Проверяем статус-код
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ошибка от сервера: %s", body)
	}

	return nil
}

// processQueue — обработка буфера
func processQueue(ctx context.Context, buffer <-chan Fact, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case fact, ok := <-buffer:
			if !ok {
				log.Println("Буфер закрыт, завершаем обработчик")
				return
			}

			log.Println("Обрабатываем запись:", fact)

			// Отправляем запись на сервер
			if err := sendFact(ctx, fact); err != nil {
				log.Printf("Ошибка отправки записи: %v", err)
			} else {
				log.Println("Запись успешно сохранена")
			}

		case <-ctx.Done():
			log.Println("Остановка обработчика буфера")
			return
		}
	}
}

// safeSendToBuffer — безопасная запись в канал с защитой от паники
func safeSendToBuffer(buffer chan Fact, fact Fact) {
	defer func() {
		if r := recover(); r != nil {
			log.Println("Паника при записи в буфер:", r)
		}
	}()

	select {
	case buffer <- fact:
		log.Println("Запись добавлена в буфер:", fact)
	default:
		log.Println("Буфер переполнен, запись пропущена")
	}
}

func main() {
	// Создаём контекст с отменой
	ctx, cancel := context.WithCancel(context.Background())

	// Ловим сигналы завершения
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	// Буфер для хранения записей
	buffer := make(chan Fact, 1000)

	// WaitGroup для ожидания завершения обработчиков
	var wg sync.WaitGroup

	// Запускаем обработчик буфера
	wg.Add(1)
	go processQueue(ctx, buffer, &wg)

	// Запускаем горутину для ожидания сигнала завершения
	go func() {
		<-signalChan
		log.Println("Получен сигнал завершения, закрываем приложение...")
		cancel()      // Отменяем контекст
		close(buffer) // Закрываем канал, чтобы завершить processQueue
	}()

	// Имитация поступления данных в буфер (пример: 10 записей)
	go func() {
		for i := 1; i <= 10; i++ {
			select {
			case <-ctx.Done():
				return
			default:
				fact := Fact{
					PeriodStart:         "2024-12-01",
					PeriodEnd:           "2024-12-31",
					PeriodKey:           "month",
					IndicatorToMoID:     227373,
					IndicatorToMoFactID: 0,
					Value:               i,
					FactTime:            "2024-12-31",
					IsPlan:              0,
					AuthUserID:          40,
					Comment:             fmt.Sprintf("buffer Trigubchak test %d", i),
				}

				// Добавляем запись в буфер
				safeSendToBuffer(buffer, fact)
			}
		}
	}()

	// Ожидаем завершения всех горутин
	wg.Wait()
	log.Println("Сервис завершил работу без ошибок")
}
